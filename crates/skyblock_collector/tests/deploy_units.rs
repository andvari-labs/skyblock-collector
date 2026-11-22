//! Tests for how the deploy kit puts its systemd units in place.

use std::env;
use std::fs::{self, Permissions};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{self, Command, Output};

/// The kit in this crate.
fn kit() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("deploy")
}

/// A fresh, empty home folder for one test, removed when dropped.
struct Home(PathBuf);

impl Home {
    fn new(name: &str) -> Self {
        let path = env::temp_dir().join(format!("skc-deploy-units-{name}-{}", process::id()));
        if path.exists() {
            fs::remove_dir_all(&path).expect("an old test home should be removable");
        }
        fs::create_dir_all(&path).expect("the test home should be creatable");
        Self(path)
    }
}

impl Drop for Home {
    fn drop(&mut self) {
        drop(fs::remove_dir_all(&self.0));
    }
}

/// Runs `link_units` from the kit's `common.sh` with `home` as `$HOME`, after writing a unit file
/// for each unit the kit installs under the deployment's `systemd/`, and running `before` first.
fn link_units(home: &Path, before: &str) {
    let output = kit_script(home, &format!("{before}\nlink_units"));
    assert!(
        output.status.success(),
        "link_units should succeed: {output:?}"
    );
}

/// Runs `body` in bash after sourcing the kit's `common.sh` with `home` as `$HOME` and writing a
/// unit file for each unit the kit installs under the deployment's `systemd/`. A stand-in
/// `systemctl` on `PATH` records its arguments in `$HOME/systemctl.log`, so no systemd is needed.
fn kit_script(home: &Path, body: &str) -> Output {
    let bin = home.join("bin");
    fs::create_dir_all(&bin).expect("the stand-in folder should be creatable");
    let systemctl = bin.join("systemctl");
    fs::write(
        &systemctl,
        "#!/bin/sh\necho \"$*\" >> \"$HOME/systemctl.log\"\n",
    )
    .expect("the stand-in should be writable");
    fs::set_permissions(&systemctl, Permissions::from_mode(0o755))
        .expect("the stand-in should be executable");
    let script = format!(
        r#"set -e
source "$KIT/common.sh"
mkdir -p "$APP/systemd/$NAME-pod.service.d" "$APP/quadlet"
for u in "${{UNITS[@]}}"; do printf '[Unit]\nDescription=%s\n' "$u" > "$APP/systemd/$u"; done
: > "$APP/systemd/$NAME-pod.service.d/restart.conf"
for q in "${{QUADLETS[@]}}"; do : > "$APP/quadlet/$q"; done
{body}
"#
    );
    let path = format!(
        "{}:{}",
        bin.display(),
        env::var("PATH").expect("PATH should be set")
    );
    Command::new("bash")
        .arg("-c")
        .arg(script)
        .env("HOME", home)
        .env("PATH", path)
        .env("KIT", kit())
        .env_remove("SKC_DEPLOY_ENV")
        .output()
        .expect("bash should start")
}

/// The units `link_units` installs, as named in the kit.
const UNITS: [&str; 6] = [
    "skyblock-collector-backup.service",
    "skyblock-collector-backup.timer",
    "skyblock-collector-update.service",
    "skyblock-collector-update.timer",
    "skyblock-collector-pull.service",
    "skyblock-collector-pull.timer",
];

/// Checks that each unit in the user's unit folder is a regular file, private to the user, with
/// the deployment's copy's contents.
fn assert_units_are_copies(home: &Path) {
    let unit_dir = home.join(".config/systemd/user");
    let app = home.join("services/skyblock-collector/systemd");
    for unit in UNITS {
        let placed = unit_dir.join(unit);
        let meta = fs::symlink_metadata(&placed).expect("each unit should be in place");
        assert!(
            meta.file_type().is_file(),
            "{unit} should be a copy, not a link: systemctl disable removes a linked unit's link"
        );
        assert_eq!(
            meta.permissions().mode() & 0o777,
            0o600,
            "{unit} should be readable by its user only"
        );
        assert_eq!(
            fs::read_to_string(&placed).expect("the copy should be readable"),
            fs::read_to_string(app.join(unit)).expect("the deployment's unit should be readable"),
            "{unit} should hold the deployment's unit"
        );
    }
}

#[test]
fn units_are_copied_not_linked() {
    let home = Home::new("fresh");
    link_units(&home.0, "");
    assert_units_are_copies(&home.0);
}

#[test]
fn a_link_from_an_older_kit_is_replaced_and_its_target_kept() {
    let home = Home::new("older");
    link_units(
        &home.0,
        r#"mkdir -p "$HOME/.config/systemd/user"
ln -s "$APP/systemd/skyblock-collector-update.timer" "$HOME/.config/systemd/user/skyblock-collector-update.timer""#,
    );
    assert_units_are_copies(&home.0);
    let target = home
        .0
        .join("services/skyblock-collector/systemd/skyblock-collector-update.timer");
    assert!(
        fs::symlink_metadata(&target)
            .expect("the deployment's unit should still exist")
            .file_type()
            .is_file(),
        "replacing the link should leave the file it pointed at in place"
    );
}

#[test]
fn running_it_again_keeps_copies() {
    let home = Home::new("again");
    link_units(&home.0, "");
    link_units(&home.0, "");
    assert_units_are_copies(&home.0);
    let leftovers = fs::read_dir(home.0.join(".config/systemd/user"))
        .expect("the unit folder should be readable")
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().contains(".new."))
        .count();
    assert_eq!(leftovers, 0, "no temporary copy should be left behind");
}

#[test]
fn units_linked_sees_an_older_kits_links_until_link_units_copies_them() {
    let home = Home::new("linked");
    let output = kit_script(
        &home.0,
        r#"mkdir -p "$HOME/.config/systemd/user"
for u in "${UNITS[@]}"; do ln -s "$APP/systemd/$u" "$HOME/.config/systemd/user/$u"; done
if units_linked; then echo linked-before; fi
link_units
if ! units_linked; then echo copies-after; fi"#,
    );
    assert!(
        output.status.success(),
        "the script should succeed: {output:?}"
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "linked-before\ncopies-after\n",
        "units_linked should see the links, and none once link_units has run"
    );
    assert_units_are_copies(&home.0);
}

#[test]
fn exactly_the_enabled_timers_are_reenabled() {
    let home = Home::new("reenable");
    link_units(
        &home.0,
        r#"mkdir -p "$HOME/.config/systemd/user/timers.target.wants"
ln -s "$APP/systemd/skyblock-collector-backup.timer" "$HOME/.config/systemd/user/timers.target.wants/skyblock-collector-backup.timer"
ln -s "$APP/systemd/skyblock-collector-pull.timer" "$HOME/.config/systemd/user/timers.target.wants/skyblock-collector-pull.timer""#,
    );
    assert_eq!(
        fs::read_to_string(home.0.join("systemctl.log"))
            .expect("systemctl should have been called"),
        "--user daemon-reload\n--user reenable skyblock-collector-backup.timer skyblock-collector-pull.timer\n",
        "the timers with enablement links, and only those, should be reenabled; nothing started or stopped"
    );
}

#[test]
fn nothing_is_reenabled_when_no_timer_is_enabled() {
    let home = Home::new("none");
    link_units(&home.0, "");
    assert_eq!(
        fs::read_to_string(home.0.join("systemctl.log"))
            .expect("systemctl should have been called"),
        "--user daemon-reload\n",
        "a timer an operator turned off should stay off"
    );
}
