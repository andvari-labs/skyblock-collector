//! Decodes the `item_bytes` blobs Hypixel attaches to auctions.
//!
//! A blob is base64 of gzipped NBT shaped like `{i: [{id, Count, Damage, tag: {ExtraAttributes, display, ...}}]}`.
//! The collector keeps three forms of every item: the exact bytes, the full NBT as JSON, and the
//! `ExtraAttributes` compound with JSON-in-string values (such as `petInfo`) parsed.

use std::io;

use base64::engine::general_purpose::STANDARD;
use fastnbt::{Value as Nbt, error};
use flate2::read::GzDecoder;
use serde_json::{Map, Number, Value as Json};

/// Upper bound on a decompressed item. Real items are a few KB; this stops a malformed blob from
/// exhausting memory.
const MAX_DECOMPRESSED: u64 = 16 * 1024 * 1024;

/// An item, with the fields the database keeps in columns of their own.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedItem {
    /// `SkyBlock` item ID from `ExtraAttributes.id`, such as `HYPERION` or `PET`.
    pub item_id: Option<String>,
    /// `ExtraAttributes.uuid`. Kept as text because Hypixel does not guarantee the format.
    pub item_uuid: Option<String>,
    /// The stack size.
    pub count: Option<i32>,
    /// `ExtraAttributes` with string values that hold JSON objects or arrays parsed in place.
    pub attributes: Option<Json>,
    /// The whole NBT document as JSON.
    pub nbt: Json,
}

/// Why an `item_bytes` blob didn't decode. The message is stored with the item.
#[derive(Debug, thiserror::Error)]
#[must_use]
pub enum DecodeError {
    /// The text isn't base64.
    #[error("item_bytes is not valid base64: {0}")]
    Base64(#[from] base64::DecodeError),
    /// The bytes aren't gzip.
    #[error("item_bytes is not valid gzip: {0}")]
    Gzip(io::Error),
    /// The item decompresses to more than `limit` bytes.
    #[error("item_bytes decompresses to more than {limit} bytes")]
    TooLarge {
        /// The most a decompressed item may hold.
        limit: u64,
    },
    /// The decompressed bytes aren't NBT.
    #[error("item_bytes is not valid NBT: {0}")]
    Nbt(#[from] error::Error),
}

/// Decodes base64 text into the gzipped bytes stored in the `item_bytes` column.
///
/// # Errors
///
/// Fails when the text, ignoring surrounding whitespace, isn't base64.
pub fn decode_base64(item_bytes: &str) -> Result<Vec<u8>, DecodeError> {
    use base64::Engine as _;

    Ok(STANDARD.decode(item_bytes.trim())?)
}

/// Decodes gzipped NBT into an item.
///
/// # Errors
///
/// Fails when the bytes aren't gzip, decompress to more than 16 MiB, or aren't NBT.
pub fn decode_gzipped(gzipped: &[u8]) -> Result<DecodedItem, DecodeError> {
    use std::io::Read as _;

    let mut raw = Vec::new();
    GzDecoder::new(gzipped)
        .take(MAX_DECOMPRESSED + 1)
        .read_to_end(&mut raw)
        .map_err(DecodeError::Gzip)?;
    if raw.len() as u64 > MAX_DECOMPRESSED {
        return Err(DecodeError::TooLarge {
            limit: MAX_DECOMPRESSED,
        });
    }
    let value: Nbt = fastnbt::from_bytes(&raw)?;
    let nbt = to_json(&value);

    let first = nbt.pointer("/i/0");
    let extra = first.and_then(|item| item.pointer("/tag/ExtraAttributes"));
    let item_id = extra
        .and_then(|e| e.get("id"))
        .and_then(Json::as_str)
        .map(str::to_owned);
    let item_uuid = extra
        .and_then(|e| e.get("uuid"))
        .and_then(Json::as_str)
        .map(str::to_owned);
    let count = first
        .and_then(|item| item.get("Count"))
        .and_then(Json::as_i64)
        .and_then(|c| i32::try_from(c).ok());
    let attributes = extra.map(expand_json_strings);

    Ok(DecodedItem {
        item_id,
        item_uuid,
        count,
        attributes,
        nbt,
    })
}

/// Converts NBT to JSON. Postgres `jsonb` rejects NUL characters and JSON has no NaN or infinity,
/// so those become a replacement character and strings respectively.
fn to_json(value: &Nbt) -> Json {
    match value {
        Nbt::Byte(v) => Json::from(*v),
        Nbt::Short(v) => Json::from(*v),
        Nbt::Int(v) => Json::from(*v),
        Nbt::Long(v) => Json::from(*v),
        Nbt::Float(v) => float(f64::from(*v)),
        Nbt::Double(v) => float(*v),
        Nbt::String(s) => Json::String(clean(s)),
        Nbt::ByteArray(a) => Json::Array(a.iter().map(|v| Json::from(*v)).collect()),
        Nbt::IntArray(a) => Json::Array(a.iter().map(|v| Json::from(*v)).collect()),
        Nbt::LongArray(a) => Json::Array(a.iter().map(|v| Json::from(*v)).collect()),
        Nbt::List(items) => Json::Array(items.iter().map(to_json).collect()),
        Nbt::Compound(map) => {
            let mut keys: Vec<_> = map.keys().collect();
            keys.sort();
            Json::Object(
                keys.into_iter()
                    .map(|k| (clean(k), to_json(&map[k])))
                    .collect(),
            )
        }
    }
}

fn float(v: f64) -> Json {
    Number::from_f64(v).map_or_else(|| Json::String(v.to_string()), Json::Number)
}

fn clean(s: &str) -> String {
    if s.contains('\0') {
        s.replace('\0', "\u{FFFD}")
    } else {
        s.to_owned()
    }
}

/// Parses string values that hold a JSON object or array, recursively. Hypixel stores some
/// attributes, such as `petInfo`, as JSON text inside NBT strings.
fn expand_json_strings(value: &Json) -> Json {
    match value {
        Json::String(s) => {
            let trimmed = s.trim_start();
            if (trimmed.starts_with('{') || trimmed.starts_with('['))
                && let Ok(parsed) = serde_json::from_str::<Json>(s)
            {
                return expand_json_strings(&parsed);
            }
            Json::String(clean(s))
        }
        Json::Array(items) => Json::Array(items.iter().map(expand_json_strings).collect()),
        Json::Object(map) => Json::Object(
            map.iter()
                .map(|(k, v)| (clean(k), expand_json_strings(v)))
                .collect::<Map<_, _>>(),
        ),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use serde_json::json;
    use std::collections::HashMap;
    use std::io::Write;

    fn gzip_nbt(value: &Nbt) -> Vec<u8> {
        let raw = fastnbt::to_bytes(value).unwrap();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&raw).unwrap();
        encoder.finish().unwrap()
    }

    fn compound(entries: HashMap<&str, Nbt>) -> Nbt {
        Nbt::Compound(
            entries
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v))
                .collect(),
        )
    }

    fn pet_item() -> Nbt {
        let extra = compound(HashMap::from([
            ("id", Nbt::String("PET".into())),
            (
                "uuid",
                Nbt::String("00000000-0000-4000-8000-000000000001".into()),
            ),
            (
                "petInfo",
                Nbt::String(
                    r#"{"type":"ENDER_DRAGON","tier":"LEGENDARY","exp":25353230.0}"#.into(),
                ),
            ),
            ("timestamp", Nbt::Long(1_794_552_250_408)),
            ("weird", Nbt::String("a\0b".into())),
            ("ratio", Nbt::Float(f32::NAN)),
        ]));
        let item = compound(HashMap::from([
            ("id", Nbt::Short(397)),
            ("Count", Nbt::Byte(1)),
            ("tag", compound(HashMap::from([("ExtraAttributes", extra)]))),
        ]));
        compound(HashMap::from([("i", Nbt::List(vec![item]))]))
    }

    #[test]
    fn decodes_pet_with_parsed_pet_info() {
        let decoded = decode_gzipped(&gzip_nbt(&pet_item())).unwrap();
        assert_eq!(decoded.item_id.as_deref(), Some("PET"));
        assert_eq!(
            decoded.item_uuid.as_deref(),
            Some("00000000-0000-4000-8000-000000000001")
        );
        assert_eq!(decoded.count, Some(1));
        let attributes = decoded.attributes.unwrap();
        assert_eq!(attributes["petInfo"]["type"], json!("ENDER_DRAGON"));
        assert_eq!(attributes["timestamp"], json!(1_794_552_250_408_i64));
        assert!(
            decoded.nbt["i"][0]["tag"]["ExtraAttributes"]["petInfo"].is_string(),
            "the full NBT keeps petInfo exactly as Hypixel sent it"
        );
    }

    #[test]
    fn sanitizes_values_postgres_jsonb_rejects() {
        let decoded = decode_gzipped(&gzip_nbt(&pet_item())).unwrap();
        let extra = &decoded.nbt["i"][0]["tag"]["ExtraAttributes"];
        assert_eq!(extra["weird"], json!("a\u{FFFD}b"));
        assert_eq!(extra["ratio"], json!("NaN"));
    }

    #[test]
    fn item_without_extra_attributes_still_decodes() {
        let item = compound(HashMap::from([
            ("id", Nbt::Short(1)),
            ("Count", Nbt::Byte(64)),
        ]));
        let root = compound(HashMap::from([("i", Nbt::List(vec![item]))]));
        let decoded = decode_gzipped(&gzip_nbt(&root)).unwrap();
        assert_eq!(decoded.item_id, None);
        assert_eq!(decoded.count, Some(64));
        assert_eq!(decoded.attributes, None);
    }

    #[test]
    fn rejects_garbage() {
        assert!(matches!(decode_base64("@@@"), Err(DecodeError::Base64(_))));
        assert!(matches!(
            decode_gzipped(b"not gzip"),
            Err(DecodeError::Gzip(_))
        ));
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(b"\x0a\x00").unwrap();
        assert!(matches!(
            decode_gzipped(&encoder.finish().unwrap()),
            Err(DecodeError::Nbt(_))
        ));
    }
}
