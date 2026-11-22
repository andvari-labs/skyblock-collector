//! A durable queue of responses waiting to be written to Postgres.
//!
//! Pollers write each new snapshot here after archiving it; the ingest worker deletes a file only
//! after its rows are committed. A crash or a database outage therefore delays rows but does not
//! lose them, and every file is processed by idempotent writes, so replaying one twice is safe.

use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::serde::rfc3339;

/// What a spooled snapshot is, which says how the database writes it. The serialized `kind` names
/// are on disk in every spool file.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Job {
    /// One `auctions_ended` snapshot.
    Ended,
    /// The first pages of `auctions`, which hold every listing changed since the last snapshot.
    /// The body is a JSON array of page bodies.
    AuctionsDelta,
    /// Every page of `auctions`. The body is a JSON array of page bodies.
    AuctionsSweep {
        /// Every page was read.
        complete: bool,
        /// Every page came from one snapshot.
        consistent: bool,
    },
    /// One `bazaar` snapshot.
    Bazaar,
    /// One `election` snapshot.
    Election,
    /// A rarely changing resource, stored whenever its content changes.
    Resource {
        /// The resource's feed name, such as `items`.
        name: String,
    },
}

/// The header line of a spool file.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Meta {
    /// What the body is.
    #[serde(flatten)]
    pub job: Job,
    /// When the body was fetched.
    #[serde(with = "rfc3339")]
    pub fetched_at: OffsetDateTime,
}

/// Whether a job may be refused when the spool is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    /// Written even when the spool is full, as sales and election snapshots are.
    Essential,
    /// Refused when the spool is full, leaving it only in the raw archive.
    Optional,
}

/// The spool directory, with `failed/` beneath it for jobs that can never be written.
#[derive(Debug)]
pub struct Spool {
    dir: PathBuf,
    failed: PathBuf,
    max_bytes: u64,
    seq: AtomicU64,
}

/// Why a job could not be spooled.
#[derive(Debug, thiserror::Error)]
#[must_use]
pub enum SpoolError {
    /// The spool is over its size limit, and the job is optional.
    #[error("spool holds {held} bytes, over its {max} byte limit")]
    Full {
        /// The bytes the spool holds.
        held: u64,
        /// The spool's limit in bytes.
        max: u64,
    },
    /// Writing the file, or reading the spool's size to check the limit, failed.
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// A job read back from the spool.
#[derive(Debug)]
pub struct Entry {
    /// The job's file.
    pub path: PathBuf,
    /// The job's header line.
    pub meta: Meta,
    /// The response body.
    pub body: Vec<u8>,
}

impl Spool {
    /// Opens the spool at `dir`, creating it and `failed/` as needed, and deletes the `.tmp`
    /// files a crash left behind.
    ///
    /// # Errors
    ///
    /// Fails when the directories can't be created or read, or a leftover can't be deleted.
    pub fn try_new(dir: PathBuf, max_bytes: u64) -> io::Result<Self> {
        let failed = dir.join("failed");
        fs::create_dir_all(&failed)?;
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "tmp") {
                fs::remove_file(path)?;
            }
        }
        Ok(Self {
            dir,
            failed,
            max_bytes,
            seq: AtomicU64::new(0),
        })
    }

    /// Writes a job durably. [`Priority::Essential`] jobs are written even when the spool is full.
    ///
    /// # Errors
    ///
    /// Fails when an optional job would take the spool over its limit, or writing fails.
    pub fn push(&self, meta: &Meta, body: &[u8], priority: Priority) -> Result<(), SpoolError> {
        use std::io::Write as _;

        if priority == Priority::Optional {
            let held = self.bytes()?;
            if held + body.len() as u64 > self.max_bytes {
                return Err(SpoolError::Full {
                    held,
                    max: self.max_bytes,
                });
            }
        }
        let name = format!(
            "{:020}-{:06}.job",
            OffsetDateTime::now_utc().unix_timestamp_nanos(),
            self.seq.fetch_add(1, Ordering::Relaxed) % 1_000_000
        );
        let tmp = self.dir.join(format!("{name}.tmp"));
        let mut file = File::create(&tmp)?;
        serde_json::to_writer(&mut file, meta).map_err(io::Error::from)?;
        file.write_all(b"\n")?;
        file.write_all(body)?;
        file.sync_all()?;
        fs::rename(&tmp, self.dir.join(name))?;
        File::open(&self.dir)?.sync_all()?;
        Ok(())
    }

    /// Pending jobs, oldest first.
    ///
    /// # Errors
    ///
    /// Fails when the spool directory can't be read.
    pub fn pending(&self) -> io::Result<Vec<PathBuf>> {
        let mut out: Vec<_> = fs::read_dir(&self.dir)?
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "job"))
            .collect();
        out.sort();
        Ok(out)
    }

    /// Reads a job file.
    ///
    /// # Errors
    ///
    /// Fails when the file can't be read or has no valid header line.
    pub fn read(path: &Path) -> io::Result<Entry> {
        use std::io::Read as _;

        let mut bytes = Vec::new();
        File::open(path)?.read_to_end(&mut bytes)?;
        let split = bytes
            .iter()
            .position(|&b| b == b'\n')
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "job has no header line"))?;
        let meta = serde_json::from_slice(&bytes[..split]).map_err(io::Error::from)?;
        let body = bytes.split_off(split + 1);
        Ok(Entry {
            path: path.to_path_buf(),
            meta,
            body,
        })
    }

    /// Deletes a job whose rows are committed.
    ///
    /// # Errors
    ///
    /// Fails when the file can't be deleted.
    pub fn complete(entry: &Entry) -> io::Result<()> {
        fs::remove_file(&entry.path)
    }

    /// Moves a job that can never succeed aside, keeping it for inspection.
    ///
    /// # Errors
    ///
    /// Fails when the path has no file name or the file can't be moved.
    pub fn quarantine(&self, path: &Path) -> io::Result<()> {
        let name = path
            .file_name()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "job path has no name"))?;
        fs::rename(path, self.failed.join(name))
    }

    /// The bytes of every file in the spool directory, quarantined jobs aside.
    ///
    /// # Errors
    ///
    /// Fails when the directory or a file's metadata can't be read.
    pub fn bytes(&self) -> io::Result<u64> {
        let mut total = 0;
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                total += entry.metadata()?.len();
            }
        }
        Ok(total)
    }

    /// How many jobs are quarantined.
    ///
    /// # Errors
    ///
    /// Fails when `failed/` can't be read.
    pub fn quarantined(&self) -> io::Result<usize> {
        Ok(fs::read_dir(&self.failed)?.count())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(job: Job) -> Meta {
        Meta {
            job,
            fetched_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn jobs_round_trip_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::try_new(dir.path().to_path_buf(), 1 << 20).unwrap();
        spool
            .push(&meta(Job::Ended), b"{\"a\":1}", Priority::Essential)
            .unwrap();
        spool
            .push(
                &meta(Job::AuctionsSweep {
                    complete: true,
                    consistent: true,
                }),
                b"[{}]",
                Priority::Optional,
            )
            .unwrap();
        let pending = spool.pending().unwrap();
        assert_eq!(pending.len(), 2);
        let first = Spool::read(&pending[0]).unwrap();
        assert_eq!(first.meta.job, Job::Ended);
        assert_eq!(first.body, b"{\"a\":1}");
        let second = Spool::read(&pending[1]).unwrap();
        assert_eq!(
            second.meta.job,
            Job::AuctionsSweep {
                complete: true,
                consistent: true,
            }
        );
        Spool::complete(&first).unwrap();
        assert_eq!(spool.pending().unwrap().len(), 1);
    }

    #[test]
    fn full_spool_refuses_optional_jobs_but_keeps_sales() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::try_new(dir.path().to_path_buf(), 100).unwrap();
        let big = vec![b'x'; 200];
        assert!(matches!(
            spool.push(&meta(Job::Bazaar), &big, Priority::Optional),
            Err(SpoolError::Full { .. })
        ));
        spool
            .push(&meta(Job::Ended), &big, Priority::Essential)
            .unwrap();
        assert_eq!(spool.pending().unwrap().len(), 1);
    }

    #[test]
    fn leftover_temp_files_are_removed_and_bad_jobs_quarantined() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("1.job.tmp"), b"partial").unwrap();
        fs::write(dir.path().join("2.job"), b"no header").unwrap();
        let spool = Spool::try_new(dir.path().to_path_buf(), 1 << 20).unwrap();
        assert!(!dir.path().join("1.job.tmp").exists());
        let pending = spool.pending().unwrap();
        assert!(Spool::read(&pending[0]).is_err());
        spool.quarantine(&pending[0]).unwrap();
        assert_eq!(spool.pending().unwrap().len(), 0);
        assert_eq!(spool.quarantined().unwrap(), 1);
    }
}
