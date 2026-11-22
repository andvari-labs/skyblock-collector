//! The raw archive: every HTTP response the collector receives, kept forever.
//!
//! Each feed writes one zstd stream per UTC day at
//! `<root>/<feed>/<YYYY-MM-DD>/<feed>-<YYYYMMDD>-<unique>.ndjson.zst`. Each line is one
//! [`Record`]. Consecutive snapshots of a feed are nearly identical, so a window with
//! long-distance matching stores each repeat for a small fraction of its size. Each new file
//! starts without that history, so files cover a day rather than an hour. A secondary collector
//! writes hourly files instead (`<feed>-<YYYYMMDD>T<HH>-<unique>`), so each is finished and can be
//! copied away within the hour.
//!
//! The open day is written to a `.partial` file and flushed and synced after every record, so a
//! crash loses at most the record being written. [`Archive::recover`] turns leftover partial
//! files into complete ones on startup.

use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, IntoInnerError, Write};
use std::path::{Path, PathBuf};
use std::str;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use time::format_description::well_known::Rfc3339;
use time::serde::rfc3339;
use time::{OffsetDateTime, UtcOffset};
use zstd::stream::read::Decoder;
use zstd::stream::write::Encoder;

use crate::feed;
use crate::http::Fetched;

/// The default decoder limit of zstd, so standard zstd tools read these files without extra
/// flags.
const WINDOW_LOG: u32 = 27;
/// A full sweep is about 90 MB of JSON, so a 128 MiB window cannot reliably reach the previous
/// one. Measured on real sweeps: 6.4x smaller with 128 MiB, 26x with 1 GiB. Reading these files
/// takes `zstd -d --long=30`.
const SWEEP_WINDOW_LOG: u32 = 30;
/// Hourly files are written on small machines. 8 MiB still reaches the previous snapshot of each
/// feed a secondary collector polls, the largest of which (bazaar) is about 3.5 MB.
const HOURLY_WINDOW_LOG: u32 = 23;
const PARTIAL: &str = ".partial";

/// One archived response. Exactly one of `body` and `body_base64` is set.
#[derive(Serialize, Deserialize, Debug)]
pub struct Record<'body> {
    /// The record format's version, 1.
    pub v: u8,
    /// Which collector process fetched it: the time that process opened the archive, in Unix
    /// nanoseconds. A change marks a restart. Records written before this field have none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<i64>,
    /// The feed the response belongs to.
    pub feed: String,
    /// The URL requested.
    pub url: String,
    /// When the request was sent.
    #[serde(with = "rfc3339")]
    pub fetched_at: OffsetDateTime,
    /// How long the request took.
    pub duration_ms: u64,
    /// The HTTP status.
    pub status: u16,
    /// The response headers the client keeps.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub headers: HashMap<String, String>,
    /// The body verbatim when it is single-line JSON.
    #[serde(borrow, default, skip_serializing_if = "Option::is_none")]
    pub body: Option<&'body RawValue>,
    /// Any other body, byte for byte.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_base64: Option<String>,
}

impl Record<'_> {
    /// The response body as it arrived.
    ///
    /// # Errors
    ///
    /// Fails when `body_base64` isn't valid base64.
    pub fn body_bytes(&self) -> io::Result<Vec<u8>> {
        match (&self.body, &self.body_base64) {
            (Some(raw), _) => Ok(raw.get().as_bytes().to_vec()),
            (None, Some(b64)) => STANDARD
                .decode(b64)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)),
            (None, None) => Ok(Vec::new()),
        }
    }
}

/// Serializes one response as an archive line, without a trailing newline.
///
/// # Panics
///
/// Never in practice: a record of strings, numbers and a valid time always serializes.
#[must_use]
pub fn encode_line(run: Option<i64>, feed: &str, fetched: &Fetched) -> Vec<u8> {
    let body = &fetched.body[..];
    let raw = str::from_utf8(body)
        .ok()
        .filter(|s| !s.contains('\n') && !s.trim().is_empty())
        .and_then(|s| serde_json::from_str::<&RawValue>(s).ok());
    let record = Record {
        v: 1,
        run,
        feed: feed.to_owned(),
        url: fetched.url.clone(),
        fetched_at: fetched.started_at,
        duration_ms: fetched.duration_ms,
        status: fetched.status,
        headers: fetched.headers.clone(),
        body: raw,
        body_base64: raw.is_none().then(|| STANDARD.encode(body)),
    };
    serde_json::to_vec(&record).expect("archive records always serialize")
}

/// How often an archive starts a new file for each feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rotation {
    /// A file per UTC day, which keeps the most history in each zstd window.
    Daily,
    /// A file per UTC hour, so each is finished and can be copied away within the hour.
    Hourly,
}

/// The current file of each feed, each behind a lock of its own.
type Slot = Mutex<Option<DayWriter>>;

/// The archive under one root directory.
pub struct Archive {
    root: PathBuf,
    level: i32,
    run: i64,
    rotation: Rotation,
    #[cfg_attr(
        dylint_lib = "trellis_lints",
        expect(
            shared_ownership,
            reason = "a writer leaves the map's lock with its feed's slot, so a slow sync on one \
                      feed doesn't hold up the others or the map"
        )
    )]
    writers: Mutex<HashMap<String, Arc<Slot>>>,
}

impl Debug for Archive {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Archive")
            .field("root", &self.root)
            .field("level", &self.level)
            .field("run", &self.run)
            .field("rotation", &self.rotation)
            .finish_non_exhaustive()
    }
}

/// The UTC day a file covers, and its hour for hourly files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Period {
    year: i32,
    month: u8,
    day: u8,
    hour: Option<u8>,
}

struct DayWriter {
    period: Period,
    partial: PathBuf,
    encoder: Encoder<'static, BufWriter<File>>,
}

impl Archive {
    /// An archive at `root` that starts a new file every UTC day, creating `root` as needed.
    ///
    /// # Errors
    ///
    /// Fails when `root` can't be created.
    pub fn try_new(root: PathBuf, level: i32) -> io::Result<Self> {
        Self::open(root, level, Rotation::Daily)
    }

    /// An archive that starts a new file every UTC hour.
    ///
    /// # Errors
    ///
    /// Fails when `root` can't be created.
    pub fn hourly(root: PathBuf, level: i32) -> io::Result<Self> {
        Self::open(root, level, Rotation::Hourly)
    }

    fn open(root: PathBuf, level: i32, rotation: Rotation) -> io::Result<Self> {
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            level,
            run: i64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos())
                .expect("Unix nanoseconds fit in i64 until 2262"),
            rotation,
            writers: Mutex::new(HashMap::new()),
        })
    }

    /// The run id this process stamps on the records it fetches.
    #[must_use]
    pub fn run(&self) -> i64 {
        self.run
    }

    /// The archive's root directory.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Appends one line to the feed's current file and makes it durable before returning.
    ///
    /// # Errors
    ///
    /// Fails when finishing the previous period's file, creating the new one, or writing and
    /// syncing the line fails.
    ///
    /// # Panics
    ///
    /// When another thread panicked while it held the archive's lock.
    pub fn append(&self, feed: &str, at: OffsetDateTime, line: &[u8]) -> io::Result<()> {
        let slot = {
            let mut writers = self.writers.lock().expect("archive lock poisoned");
            writers.entry(feed.to_owned()).or_default().clone()
        };
        let mut slot = slot.lock().expect("archive feed lock poisoned");
        let at = at.to_offset(UtcOffset::UTC);
        let period = Period {
            year: at.year(),
            month: u8::from(at.month()),
            day: at.day(),
            hour: (self.rotation == Rotation::Hourly).then(|| at.hour()),
        };
        if slot.as_ref().is_some_and(|w| w.period != period) {
            slot.take().expect("checked above").finish()?;
        }
        if slot.is_none() {
            *slot = Some(DayWriter::open(&self.root, feed, period, self.level)?);
        }
        let writer = slot.as_mut().expect("opened above");
        writer.encoder.write_all(line)?;
        writer.encoder.write_all(b"\n")?;
        writer.encoder.flush()?;
        writer.encoder.get_mut().flush()?;
        writer.encoder.get_ref().get_ref().sync_data()
    }

    /// Closes every open day. Called on shutdown.
    ///
    /// # Errors
    ///
    /// Fails at the first file that can't be finished, leaving the rest open.
    ///
    /// # Panics
    ///
    /// When another thread panicked while it held the archive's lock.
    pub fn finish_all(&self) -> io::Result<()> {
        let writers: Vec<_> = self
            .writers
            .lock()
            .expect("archive lock poisoned")
            .values()
            .cloned()
            .collect();
        for slot in writers {
            if let Some(writer) = slot.lock().expect("archive feed lock poisoned").take() {
                writer.finish()?;
            }
        }
        Ok(())
    }

    /// The `.partial` files left by an unclean stop. List them before writing anything, so the
    /// files this process opens are not mistaken for leftovers.
    ///
    /// # Errors
    ///
    /// Fails when a directory under the root can't be read.
    pub fn leftover_partials(&self) -> io::Result<Vec<PathBuf>> {
        Ok(walk(&self.root)?
            .into_iter()
            .filter(|p| p.to_str().is_some_and(|s| s.ends_with(PARTIAL)))
            .collect())
    }

    /// Rewrites leftover `.partial` files into complete files holding every whole line, streaming
    /// so memory stays bounded by one line. Returns the number of files recovered.
    ///
    /// # Errors
    ///
    /// Fails at the first path that isn't a `.partial` file or can't be rewritten; the files
    /// before it are recovered.
    ///
    /// # Panics
    ///
    /// When a recovered file has no parent directory, which an archive file always has.
    pub fn recover(&self, partials: &[PathBuf]) -> io::Result<usize> {
        for path in partials {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_suffix(PARTIAL))
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "not a partial file"))?;
            let target = path.with_file_name(name.replace(".ndjson.zst", "-recovered.ndjson.zst"));
            let tmp = target.with_extension("zst.tmp");
            let feed = path
                .parent()
                .and_then(Path::parent)
                .and_then(Path::file_name)
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            let window = window_log(feed, self.rotation);
            let mut encoder = new_encoder(File::create(&tmp)?, self.level, window)?;
            let written = copy_whole_lines(path, &mut encoder)?;
            let file = encoder
                .finish()?
                .into_inner()
                .map_err(IntoInnerError::into_error)?;
            if written > 0 {
                file.sync_all()?;
                fs::rename(&tmp, &target)?;
                make_readonly(&target)?;
            } else {
                drop(file);
                fs::remove_file(&tmp)?;
            }
            fs::remove_file(path)?;
            sync_dir(target.parent().expect("archive files have a parent"))?;
        }
        Ok(partials.len())
    }
}

impl DayWriter {
    fn open(root: &Path, feed: &str, period: Period, level: i32) -> io::Result<Self> {
        let Period {
            year,
            month,
            day,
            hour,
        } = period;
        let dir = root
            .join(feed)
            .join(format!("{year:04}-{month:02}-{day:02}"));
        fs::create_dir_all(&dir)?;
        let unique = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let suffix = hour.map_or_default(|h| format!("T{h:02}"));
        let partial = dir.join(format!(
            "{feed}-{year:04}{month:02}{day:02}{suffix}-{unique}.ndjson.zst{PARTIAL}"
        ));
        let file = File::options()
            .create_new(true)
            .write(true)
            .open(&partial)?;
        sync_dir(&dir)?;
        let rotation = if hour.is_some() {
            Rotation::Hourly
        } else {
            Rotation::Daily
        };
        Ok(Self {
            period,
            partial,
            encoder: new_encoder(file, level, window_log(feed, rotation))?,
        })
    }

    fn finish(self) -> io::Result<()> {
        let file = self
            .encoder
            .finish()?
            .into_inner()
            .map_err(IntoInnerError::into_error)?;
        file.sync_all()?;
        let done = self.partial.with_file_name(
            self.partial
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_suffix(PARTIAL))
                .expect("partial files end in .partial"),
        );
        fs::rename(&self.partial, &done)?;
        make_readonly(&done)?;
        sync_dir(done.parent().expect("archive files have a parent"))
    }
}

fn window_log(feed: &str, rotation: Rotation) -> u32 {
    if rotation == Rotation::Hourly {
        HOURLY_WINDOW_LOG
    } else if feed == feed::SWEEP {
        SWEEP_WINDOW_LOG
    } else {
        WINDOW_LOG
    }
}

fn new_encoder(
    file: File,
    level: i32,
    window_log: u32,
) -> io::Result<Encoder<'static, BufWriter<File>>> {
    let mut encoder = Encoder::new(BufWriter::new(file), level)?;
    encoder.window_log(window_log)?;
    encoder.long_distance_matching(true)?;
    encoder.include_checksum(true)?;
    Ok(encoder)
}

fn new_decoder(path: &Path) -> io::Result<Decoder<'static, BufReader<File>>> {
    let mut decoder = Decoder::new(File::open(path)?)?;
    decoder.window_log_max(SWEEP_WINDOW_LOG)?;
    Ok(decoder)
}

/// Copies every complete line of a possibly truncated zstd stream into `out`. Returns the bytes
/// copied. A truncated stream ends in a decode error, which marks the end of what survived.
fn copy_whole_lines(path: &Path, out: &mut impl Write) -> io::Result<u64> {
    use std::io::Read as _;

    let mut decoder = new_decoder(path)?;
    let mut buf = vec![0u8; 1 << 20];
    let mut pending = Vec::new();
    let mut written = 0;
    loop {
        match decoder.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => pending.extend_from_slice(&buf[..n]),
        }
        if let Some(end) = pending.iter().rposition(|&b| b == b'\n') {
            out.write_all(&pending[..=end])?;
            written += end as u64 + 1;
            pending.drain(..=end);
        }
    }
    Ok(written)
}

/// Streams the lines of a complete archive file.
///
/// # Errors
///
/// Fails when the file can't be opened. Each line is a result of its own, so a file that isn't a
/// zstd stream fails at its first line.
pub fn read_lines(path: &Path) -> io::Result<impl Iterator<Item = io::Result<Vec<u8>>> + use<>> {
    use std::io::BufRead as _;

    let decoder = new_decoder(path)?;
    Ok(BufReader::with_capacity(1 << 20, decoder).split(b'\n'))
}

fn walk(root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_dir() {
                stack.push(entry.path());
            } else if kind.is_file() {
                out.push(entry.path());
            }
        }
    }
    out.sort();
    Ok(out)
}

/// Every complete archive file under `root`, oldest first.
///
/// # Errors
///
/// Fails when a directory under `root` can't be read.
pub fn list_files(root: &Path) -> io::Result<Vec<PathBuf>> {
    Ok(walk(root)?
        .into_iter()
        .filter(|p| p.to_str().is_some_and(|s| s.ends_with(".ndjson.zst")))
        .collect())
}

/// Finished archive files never change again.
fn make_readonly(path: &Path) -> io::Result<()> {
    let mut perms = fs::metadata(path)?.permissions();
    perms.set_readonly(true);
    fs::set_permissions(path, perms)
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Formats a time as RFC 3339, or an empty string for a time RFC 3339 can't hold.
#[must_use]
pub fn format_time(t: OffsetDateTime) -> String {
    t.format(&Rfc3339).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::mem;

    use bytes::Bytes;
    use time::macros::datetime;

    /// Drops the archive without finishing its streams, as a crash would, leaving each open file
    /// partial for recovery to write again.
    fn crash(archive: Archive) {
        mem::forget(archive);
    }

    fn line(body: &[u8]) -> Vec<u8> {
        let fetched = Fetched {
            url: "https://api.hypixel.net/v2/skyblock/bazaar".to_owned(),
            started_at: datetime!(2026-11-13 07:12:09 UTC),
            duration_ms: 12,
            status: 200,
            headers: HashMap::from([("age".to_owned(), "3".to_owned())]),
            body: Bytes::copy_from_slice(body),
        };
        encode_line(None, "bazaar", &fetched)
    }

    #[test]
    fn json_bodies_are_embedded_verbatim_and_others_are_base64() {
        let json = br#"{"success":true,"lastUpdated":1}"#;
        let encoded = line(json);
        let record: Record<'_> = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(record.body.unwrap().get().as_bytes(), json);
        assert_eq!(record.body_bytes().unwrap(), json);

        let html = b"<html>\n502 Bad Gateway</html>";
        let encoded = line(html);
        let record: Record<'_> = serde_json::from_slice(&encoded).unwrap();
        assert!(record.body.is_none());
        assert_eq!(record.body_bytes().unwrap(), html);
    }

    #[test]
    fn days_rotate_into_complete_readonly_files() {
        let dir = tempfile::tempdir().unwrap();
        let archive = Archive::try_new(dir.path().to_path_buf(), 3).unwrap();
        archive
            .append(
                "bazaar",
                datetime!(2026-11-13 23:59:59 UTC),
                &line(b"{\"a\":1}"),
            )
            .unwrap();
        archive
            .append(
                "bazaar",
                datetime!(2026-11-14 00:00:01 UTC),
                &line(b"{\"a\":2}"),
            )
            .unwrap();
        archive
            .append(
                "bazaar",
                datetime!(2026-11-14 18:00:00 UTC),
                &line(b"{\"a\":3}"),
            )
            .unwrap();
        archive.finish_all().unwrap();
        let files = list_files(dir.path()).unwrap();
        assert_eq!(files.len(), 2, "one file per UTC day");
        for file in &files {
            assert!(fs::metadata(file).unwrap().permissions().readonly());
        }
        let lines = |f: &Path| {
            read_lines(f)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .len()
        };
        assert_eq!(lines(&files[0]), 1);
        assert_eq!(lines(&files[1]), 2);
    }

    #[test]
    fn hourly_archives_finish_a_file_each_hour() {
        let dir = tempfile::tempdir().unwrap();
        let archive = Archive::hourly(dir.path().to_path_buf(), 3).unwrap();
        for at in [
            datetime!(2026-11-13 22:10:00 UTC),
            datetime!(2026-11-13 22:59:59 UTC),
            datetime!(2026-11-13 23:00:01 UTC),
        ] {
            archive.append("bazaar", at, &line(b"{\"a\":1}")).unwrap();
        }
        let finished = list_files(dir.path()).unwrap();
        assert_eq!(finished.len(), 1, "the 22:00 hour is finished at 23:00");
        let name = finished[0].file_name().unwrap().to_str().unwrap();
        assert!(name.starts_with("bazaar-20261113T22-"), "{name}");
        assert_eq!(read_lines(&finished[0]).unwrap().count(), 2);
        assert_eq!(archive.leftover_partials().unwrap().len(), 1);
    }

    #[test]
    fn crash_leftovers_are_recovered_with_every_whole_line() {
        let dir = tempfile::tempdir().unwrap();
        let archive = Archive::try_new(dir.path().to_path_buf(), 3).unwrap();
        for n in 0..3 {
            let body = format!("{{\"n\":{n}}}");
            archive
                .append(
                    "ended",
                    datetime!(2026-11-13 07:00:00 UTC),
                    &line(body.as_bytes()),
                )
                .unwrap();
        }
        crash(archive);
        let archive = Archive::try_new(dir.path().to_path_buf(), 3).unwrap();
        let leftovers = archive.leftover_partials().unwrap();
        assert_eq!(archive.recover(&leftovers).unwrap(), 1);
        assert_eq!(archive.leftover_partials().unwrap(), Vec::<PathBuf>::new());
        let files = list_files(dir.path()).unwrap();
        assert_eq!(files.len(), 1);
        assert!(
            files[0]
                .to_str()
                .unwrap()
                .ends_with("-recovered.ndjson.zst")
        );
        let lines: Vec<_> = read_lines(&files[0])
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(lines.len(), 3);
        let last: Record<'_> = serde_json::from_slice(&lines[2]).unwrap();
        assert_eq!(last.body.unwrap().get(), "{\"n\":2}");
    }

    /// Sweep files declare the 1 GiB window, which a decoder at zstd's default limit refuses, and
    /// the archive's own readers accept. Other feeds stay within the default limit.
    #[test]
    fn sweep_files_use_the_large_window_and_stay_readable() {
        let plain_read = |f: &Path| {
            let mut out = Vec::new();
            Decoder::new(File::open(f).unwrap())
                .unwrap()
                .read_to_end(&mut out)
                .map(|_| out)
        };
        let dir = tempfile::tempdir().unwrap();
        let archive = Archive::try_new(dir.path().to_path_buf(), 3).unwrap();
        let at = datetime!(2026-11-13 07:00:00 UTC);
        archive.append("bazaar", at, &line(b"{\"a\":1}")).unwrap();
        archive.finish_all().unwrap();
        archive
            .append(feed::SWEEP, at, &line(b"{\"a\":2}"))
            .unwrap();
        crash(archive);
        let archive = Archive::try_new(dir.path().to_path_buf(), 3).unwrap();
        let leftovers = archive.leftover_partials().unwrap();
        assert_eq!(archive.recover(&leftovers).unwrap(), 1);

        let files = list_files(dir.path()).unwrap();
        assert_eq!(files.len(), 2);
        for file in &files {
            let is_sweep = file.to_str().unwrap().contains("auctions_sweep-");
            assert_eq!(plain_read(file).is_err(), is_sweep, "{}", file.display());
            assert_eq!(read_lines(file).unwrap().count(), 1);
        }
    }
}
