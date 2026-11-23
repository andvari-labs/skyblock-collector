//! Tests for how the deploy kit's `restore.sh` treats the revision from before the first release.

use std::env;
use std::fs::{self, Permissions};
use std::path::{Path, PathBuf};
use std::process::{self, Command, Output};

/// The revision a test deployment records in `pre-trellis-rev`.
const PRE_RELEASE: &str = "1111111111111111111111111111111111111111";

/// What `podman image exists` answers for `localhost/skyblock-collector:pre-trellis`.
#[derive(Clone, Copy)]
enum PreTrellisImage {
    Kept,
    Removed,
    /// Podman itself fails, as with a storage error.
    CheckFails,
}

/// A fresh home folder for one test, holding a deployment with `pre-trellis-rev` and stand-ins
/// for the commands `restore.sh` runs, removed when dropped. Each stand-in records its arguments
/// in `$HOME/calls.log`. `podman` answers `image exists` from `$IMAGE_EXISTS` and succeeds at
/// everything else; `git`, `systemctl` and `curl` fail, so a run can never reach the host's own
/// clone, units or status server.
struct Home(PathBuf);

impl Home {
    fn new(name: &str) -> Self {
        use std::os::unix::fs::PermissionsExt;

        let path = env::temp_dir().join(format!("skc-deploy-restore-{name}-{}", process::id()));
        if path.exists() {
            fs::remove_dir_all(&path).expect("an old test home should be removable");
        }
        let app = path.join("services/skyblock-collector");
        fs::create_dir_all(&app).expect("the test deployment should be creatable");
        fs::write(app.join("pre-trellis-rev"), format!("{PRE_RELEASE}\n"))
            .expect("pre-trellis-rev should be writable");
        let bin = path.join("bin");
        fs::create_dir_all(&bin).expect("the stand-in folder should be creatable");
        let stand_ins = [
            (
                "podman",
                "if [ \"$1 $2\" = 'image exists' ]; then exit \"$IMAGE_EXISTS\"; fi\nexit 0",
            ),
            ("git", "exit 1"),
            ("systemctl", "exit 1"),
            ("curl", "exit 1"),
        ];
        for (command, body) in stand_ins {
            let file = bin.join(command);
            fs::write(
                &file,
                format!("#!/bin/sh\necho \"{command} $*\" >> \"$HOME/calls.log\"\n{body}\n"),
            )
            .expect("the stand-in should be writable");
            fs::set_permissions(&file, Permissions::from_mode(0o755))
                .expect("the stand-in should be executable");
        }
        Self(path)
    }

    /// Runs the kit's `restore.sh --rebuild <revision>`, with the pre-trellis image as `image`
    /// says.
    fn rebuild(&self, revision: &str, image: PreTrellisImage) -> Output {
        let path = format!(
            "{}:{}",
            self.0.join("bin").display(),
            env::var("PATH").expect("PATH should be set")
        );
        Command::new("bash")
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("deploy/restore.sh"))
            .args(["--rebuild", revision])
            .env("HOME", &self.0)
            .env("PATH", path)
            .env(
                "IMAGE_EXISTS",
                match image {
                    PreTrellisImage::Kept => "0",
                    PreTrellisImage::Removed => "1",
                    PreTrellisImage::CheckFails => "125",
                },
            )
            .env_remove("SKC_DEPLOY_ENV")
            .output()
            .expect("bash should start")
    }

    fn calls(&self) -> String {
        fs::read_to_string(self.0.join("calls.log")).unwrap_or_default()
    }

    fn locked(&self) -> bool {
        self.0
            .join("services/skyblock-collector/backups/.lock")
            .exists()
    }
}

impl Drop for Home {
    fn drop(&mut self) {
        drop(fs::remove_dir_all(&self.0));
    }
}

const IMAGE_CHECK: &str = "podman image exists localhost/skyblock-collector:pre-trellis";

#[test]
fn the_pre_release_revision_without_its_image_is_refused_before_anything_starts() {
    for revision in [&PRE_RELEASE[..7], PRE_RELEASE] {
        let home = Home::new("removed");
        let output = home.rebuild(revision, PreTrellisImage::Removed);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            !output.status.success(),
            "the rebuild should fail: {output:?}"
        );
        assert!(
            stderr.contains("localhost/skyblock-collector:pre-trellis, was removed")
                && stderr.contains("pass a release tag"),
            "the message should say the image was removed and to pass a release tag: {stderr}"
        );
        assert_eq!(
            home.calls(),
            format!("{IMAGE_CHECK}\n"),
            "nothing but the image check should run"
        );
        assert!(!home.locked(), "the lock should not be taken");
    }
}

#[test]
fn a_failing_image_check_is_not_reported_as_a_removed_image() {
    let home = Home::new("check-fails");
    let output = home.rebuild(PRE_RELEASE, PreTrellisImage::CheckFails);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("could not check for localhost/skyblock-collector:pre-trellis")
            && !stderr.contains("was removed"),
        "podman's own failure should be named as such: {stderr}"
    );
    assert!(!home.locked(), "the lock should not be taken");
}

#[test]
fn the_pre_release_revision_with_its_image_passes_the_check_and_takes_the_lock() {
    let home = Home::new("kept");
    let output = home.rebuild(PRE_RELEASE, PreTrellisImage::Kept);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("was removed"),
        "a kept image should pass the check: {stderr}"
    );
    assert!(
        home.locked(),
        "the restore should take the lock and go on: {stderr}"
    );
    assert!(
        home.calls().starts_with(&format!("{IMAGE_CHECK}\n")),
        "the image should be checked before anything else: {}",
        home.calls()
    );
}

#[test]
fn a_release_a_short_prefix_and_another_commit_are_not_held_up_by_the_missing_image() {
    for revision in ["v0.2.0", &PRE_RELEASE[..6], "2222222222"] {
        let home = Home::new("other");
        let output = home.rebuild(revision, PreTrellisImage::Removed);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            !stderr.contains("was removed") && home.locked(),
            "{revision} should go on to the restore: {stderr}"
        );
        assert!(
            !home.calls().contains(IMAGE_CHECK),
            "{revision} should not look for the pre-trellis image: {}",
            home.calls()
        );
    }
}
