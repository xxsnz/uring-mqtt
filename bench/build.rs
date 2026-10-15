//! Build-time source identity for the bench crate (task 9). The runner
//! embeds the commit and clean/dirty status of the source the executable
//! was built from into the binary, so a recorded-profile `bench run` can
//! refuse a binary whose checkout has moved (`runner::check_provenance`,
//! AC-57). Reading the parent crate's git state keeps the bench crate's
//! own commits out of the check.

use std::io::{self, Read};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

/// Upper bound on the reap after a probe's kill. A kill can be refused, so
/// the probe stops waiting then; a command still running is left to init,
/// which reaps it once the build script exits.
const GIT_PROBE_REAP_TIMEOUT_SECS: u64 = 1;

/// Upper bound on one build-time `git` probe. A `git` that never answers
/// must not stall the compile: the probe gives up and the value it would have
/// produced is `unknown`. Mirrors `runner::GIT_HEAD_TIMEOUT_SECS`.
const GIT_PROBE_TIMEOUT_SECS: u64 = 5;

/// Poll interval of the probe's exit check.
const GIT_PROBE_POLL_MS: u64 = 50;

/// Cap on the stdout bytes a probe keeps. Real output is a SHA or one line
/// per changed path; more than this is a failed probe, so a `git` that prints
/// without stopping cannot grow the build script's memory.
const GIT_PROBE_STDOUT_BYTES_MAX: u64 = 1 << 20;

/// The stdout of `command`, or `None` when it could not run, did not exit 0,
/// printed more than [`GIT_PROBE_STDOUT_BYTES_MAX`] bytes, or did not start
/// and finish within [`GIT_PROBE_TIMEOUT_SECS`]. Startup runs on its own
/// thread, so a `git` — or a `PATH` entry ahead of it — on a stalled
/// filesystem cannot hold the build inside a synchronous `spawn`; the
/// starting thread keeps the command and hands it to [`abandon`] when
/// startup finishes after this function gave up — while the build script
/// lives. A startup that finishes after it exits leaves a read-only `git`
/// to init, unkilled; `main`'s `cargo:warning` reports every give-up, and
/// killing a command whose pid `spawn` has not yet returned needs a
/// `pre_exec` the crate's `unsafe_code = "forbid"` rules out.
/// Stdout is read on its own
/// thread too, so a descendant that inherits the pipe and holds it open past
/// the deadline cannot hold the build either; a command still running at the
/// deadline is handed to [`abandon`] (`kill` is `Child::kill` in production).
/// Descendants of a wrapper `git` are not killed — a group signal needs an
/// unstable std API or a build dependency — but none of them can extend the
/// bound.
pub fn bounded_stdout<K: FnMut(&mut Child) -> io::Result<()> + Clone + Send + 'static>(
    mut command: Command,
    mut kill: K,
) -> Option<Vec<u8>> {
    let deadline = Instant::now() + Duration::from_secs(GIT_PROBE_TIMEOUT_SECS);
    // A rendezvous channel, so a send that completes proves this function
    // received the command: a queued one it never took would be dropped
    // without a kill or a reap when this function returns.
    let (started_tx, started_rx) = mpsc::sync_channel(0);
    let mut start_kill = kill.clone();
    let starter = thread::Builder::new().spawn(move || {
        let spawned = command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn();
        if let Err(mpsc::SendError(Ok(mut child))) = started_tx.send(spawned) {
            abandon(&mut child, &mut start_kill);
        }
    });
    if starter.is_err() {
        return None;
    }
    let startup = deadline.saturating_duration_since(Instant::now());
    let Ok(Ok(mut child)) = started_rx.recv_timeout(startup) else {
        return None;
    };
    let stdout = child.stdout.take();
    let (tx, rx) = mpsc::channel();
    let reader = thread::Builder::new().spawn(move || {
        let Some(stdout) = stdout else { return };
        let mut kept = Vec::new();
        let read = stdout
            .take(GIT_PROBE_STDOUT_BYTES_MAX + 1)
            .read_to_end(&mut kept);
        let _ = tx.send(read.map(|_| kept));
    });
    if reader.is_err() {
        abandon(&mut child, &mut kill);
        return None;
    }
    let exited = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break None;
        }
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) => thread::sleep(remaining.min(Duration::from_millis(GIT_PROBE_POLL_MS))),
            Err(_) => break None,
        }
    };
    let Some(status) = exited else {
        abandon(&mut child, &mut kill);
        return None;
    };
    if !status.success() {
        return None;
    }
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return None;
    }
    let kept = rx.recv_timeout(remaining).ok()?.ok()?;
    if Instant::now() > deadline {
        return None;
    }
    let within_cap =
        u64::try_from(kept.len()).is_ok_and(|n| n <= GIT_PROBE_STDOUT_BYTES_MAX);
    within_cap.then_some(kept)
}

/// End a command [`bounded_stdout`] gave up on: `kill` it, then reap it if it
/// exits within [`GIT_PROBE_REAP_TIMEOUT_SECS`]. std's `Child::kill` does not
/// signal a child std has already reaped, so the pid cannot name a stranger;
/// a command still running at the bound — its kill refused — is left to init,
/// which reaps it once the build script exits.
fn abandon<K: FnMut(&mut Child) -> io::Result<()>>(child: &mut Child, kill: &mut K) {
    let _ = kill(child);
    let reap_deadline = Instant::now() + Duration::from_secs(GIT_PROBE_REAP_TIMEOUT_SECS);
    while matches!(child.try_wait(), Ok(None)) && Instant::now() < reap_deadline {
        thread::sleep(Duration::from_millis(GIT_PROBE_POLL_MS));
    }
}

/// `git <args>` run on the parent crate, whose state the binary embeds.
/// `--no-optional-locks` keeps `git status` from taking the index lock, so a
/// probe killed at its deadline cannot leave a stale `.git/index.lock`. It is
/// a global option: git rejects it after the subcommand, so the order this
/// builds is what makes the probe answer at all, and
/// `bench/tests/runner.rs` runs the checkout's own `git` through it.
pub fn git(args: &[&str]) -> Command {
    let mut command = Command::new("git");
    command.arg("--no-optional-locks").args(args).current_dir("..");
    command
}

fn main() {
    let short = bounded_stdout(git(&["rev-parse", "--short", "HEAD"]), Child::kill)
        .and_then(|stdout| String::from_utf8(stdout).ok())
        .map(|s| s.trim().to_string());
    if short.is_none() {
        println!("cargo:warning=git rev-parse --short HEAD failed or gave up after {GIT_PROBE_TIMEOUT_SECS}s; BENCH_SOURCE_SHA=unknown until bench/build.rs is touched and bench rebuilt");
    }
    println!(
        "cargo:rustc-env=BENCH_SOURCE_SHA={}",
        short.as_deref().unwrap_or("unknown")
    );

    // A probe that could not run, or that failed, says nothing about the
    // tree: reporting `false` there would let a dirty checkout with an
    // unreadable index pass `check_provenance`. `unknown` is the same
    // honest third state the SHA uses, and the runner reads anything but
    // `false` as dirty.
    let dirty = match bounded_stdout(git(&["status", "--porcelain"]), Child::kill) {
        Some(stdout) => {
            if stdout.is_empty() { "false" } else { "true" }
        }
        None => "unknown",
    };
    if dirty == "unknown" {
        println!("cargo:warning=git status --porcelain failed or gave up after {GIT_PROBE_TIMEOUT_SECS}s; BENCH_SOURCE_DIRTY=unknown until bench/build.rs is touched and bench rebuilt");
    }
    println!("cargo:rustc-env=BENCH_SOURCE_DIRTY={dirty}");

    println!("cargo:rerun-if-changed=../.git/HEAD");
    println!("cargo:rerun-if-changed=../.git/index");
    println!("cargo:rerun-if-changed=../.git/refs/heads");
    println!("cargo:rerun-if-changed=../src");
    println!("cargo:rerun-if-changed=../Cargo.toml");
    println!("cargo:rerun-if-changed=../Cargo.lock");
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=Cargo.lock");
    println!("cargo:rerun-if-changed=build.rs");
}