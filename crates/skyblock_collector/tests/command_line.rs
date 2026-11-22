//! Tests for the command line of `skyblock-collector`.

use std::process::{Command, Output};

/// Runs the built `skyblock-collector` with `arguments`.
fn run(arguments: &[&str]) -> Output {
    let binary = env!("CARGO_BIN_EXE_skyblock-collector");
    Command::new(binary)
        .args(arguments)
        .output()
        .expect("the binary should start")
}

#[test]
fn version_prints_the_version_line_and_succeeds() {
    let output = run(&["--version"]);
    assert!(
        output.status.success(),
        "CI's Container image job runs the image with `--version` and needs exit 0: {output:?}"
    );
    let name = "skyblock-collector";
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("{name} {}\n", env!("CARGO_PKG_VERSION")),
        "`--version` should print the name and the version"
    );
}

#[test]
fn an_unknown_argument_fails() {
    let output = run(&["--unknown"]);
    assert_eq!(
        output.status.code(),
        Some(2),
        "an argument the service doesn't know should fail: {output:?}"
    );
}

/// Each case's arguments, the file under `tests/command_line/` holding what the binary must print
/// (standard output, then standard error), and its exit code. The files are the old
/// `skyblock-collector`'s output for the same arguments, byte for byte, so scripts that read the
/// help or the errors keep working.
const RECORDED: [(&[&str], &str, i32); 12] = [
    (&["--help"], include_str!("command_line/long-help.txt"), 0),
    (&["-h"], include_str!("command_line/short-help.txt"), 0),
    (&["help"], include_str!("command_line/help.txt"), 0),
    (
        &["help", "replay"],
        include_str!("command_line/help-replay.txt"),
        0,
    ),
    (
        &["run", "--help"],
        include_str!("command_line/run-help.txt"),
        0,
    ),
    (
        &["secondary", "--help"],
        include_str!("command_line/secondary-help.txt"),
        0,
    ),
    (
        &["replay", "--help"],
        include_str!("command_line/replay-help.txt"),
        0,
    ),
    (
        &["backfill", "--help"],
        include_str!("command_line/backfill-help.txt"),
        0,
    ),
    (
        &["recover-archive", "--help"],
        include_str!("command_line/recover-archive-help.txt"),
        0,
    ),
    (
        &["health", "--help"],
        include_str!("command_line/health-help.txt"),
        0,
    ),
    (&["bogus"], include_str!("command_line/bogus.txt"), 2),
    (&["--bogus"], include_str!("command_line/flag-bogus.txt"), 2),
];

#[test]
fn help_and_errors_print_what_the_old_binary_printed() {
    for (arguments, expected, code) in RECORDED {
        let output = run(arguments);
        let printed = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(printed, expected, "the output of {arguments:?}");
        assert_eq!(
            output.status.code(),
            Some(code),
            "the exit code of {arguments:?}"
        );
    }
}
