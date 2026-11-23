//! Keeps identifiers in the workspace's Rust sources plainly invented: every 32-digit hex string or
//! hyphenated UUID in a `.rs` file must use at most four distinct characters, such as
//! `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1` or `00000000-0000-4000-8000-000000000001`. A random-looking
//! one could be a real player, auction or item ID copied from a response. The synthetic fixtures
//! under `tests/fixtures/` are JSON and outside this check.

use std::fs;
use std::path::{Path, PathBuf};

/// The `.rs` files under `dir`, recursively, leaving out build output and hidden folders. Symbolic
/// links are not followed.
fn rust_files(dir: &Path, found: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(dir).expect("each source folder should be readable");
    for entry in entries {
        let entry = entry.expect("each source folder's entries should be readable");
        let path = entry.path();
        let name = entry.file_name();
        let kind = entry
            .file_type()
            .expect("each entry's type should be readable");
        if kind.is_dir() && name != "target" && !name.to_string_lossy().starts_with('.') {
            rust_files(&path, found);
        } else if kind.is_file() && path.extension().is_some_and(|ext| ext == "rs") {
            found.push(path);
        }
    }
}

/// An identifier-shaped string and the line it is on.
#[derive(Debug, PartialEq, Eq)]
struct Found {
    line: usize,
    id: String,
}

/// The identifier-shaped strings in `text`: a group of exactly 32 hex digits, or five hyphenated
/// groups of 8, 4, 4, 4 and 12, even inside a longer hyphenated name.
fn identifiers(text: &str) -> Vec<Found> {
    let mut found = Vec::new();
    for (index, line) in text.lines().enumerate() {
        for token in line.split(|c: char| !(c.is_ascii_hexdigit() || c == '-')) {
            let groups: Vec<&str> = token.split('-').collect();
            for (start, group) in groups.iter().enumerate() {
                if group.len() == 32 {
                    found.push(Found {
                        line: index + 1,
                        id: (*group).to_owned(),
                    });
                }
                let lengths: Vec<usize> =
                    groups.iter().skip(start).take(5).map(|g| g.len()).collect();
                if lengths == [8, 4, 4, 4, 12] {
                    found.push(Found {
                        line: index + 1,
                        id: groups[start..start + 5].join("-"),
                    });
                }
            }
        }
    }
    found
}

#[test]
fn identifiers_in_rust_sources_are_plainly_invented() {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crate should sit in the workspace's crates folder");
    let mut files = Vec::new();
    rust_files(crates, &mut files);
    assert!(
        !files.is_empty(),
        "the workspace's Rust sources should be found"
    );
    let mut offenders = Vec::new();
    for file in &files {
        let text = fs::read_to_string(file).expect("each Rust source should be readable");
        for Found { line, id } in identifiers(&text) {
            let mut distinct: Vec<char> = id.chars().filter(|c| *c != '-').collect();
            distinct.sort_unstable();
            distinct.dedup();
            if distinct.len() > 4 {
                offenders.push(format!(
                    "{}:{line}: an identifier with {} distinct characters",
                    file.display(),
                    distinct.len()
                ));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "replace these with plainly invented identifiers (at most 4 distinct characters), \
         so no real ID is copied into the sources: {offenders:#?}"
    );
}

#[test]
fn identifiers_finds_hex_runs_and_uuids_only() {
    let text = "x aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1 y 00000000-0000-4000-8000-000000000001\n\
                a checksum 0000000000000000000000000000000000000000000000000000000000000000\n\
                auction-11111111-1111-4111-8111-111111111111.json bad-cccccccccccccccccccccccccccccccc-x";
    let found = identifiers(text);
    assert_eq!(
        found,
        [
            Found {
                line: 1,
                id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1".to_owned(),
            },
            Found {
                line: 1,
                id: "00000000-0000-4000-8000-000000000001".to_owned(),
            },
            Found {
                line: 3,
                id: "11111111-1111-4111-8111-111111111111".to_owned(),
            },
            Found {
                line: 3,
                id: "cccccccccccccccccccccccccccccccc".to_owned(),
            },
        ],
        "hex runs and UUIDs are found inside hyphenated names, and a 64-digit checksum is not one"
    );
}
