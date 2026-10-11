//! Build-time source identity for the bench crate (task 9). The runner
//! embeds the commit and clean/dirty status of the source the executable
//! was built from into the binary, so a recorded-profile `bench run` can
//! refuse a binary whose checkout has moved (`runner::check_provenance`,
//! AC-57). Reading the parent crate's git state keeps the bench crate's
//! own commits out of the check.

use std::process::Command;

fn main() {
    let short = Command::new("git")
        .arg("rev-parse")
        .arg("--short")
        .arg("HEAD")
        .current_dir("..")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string());
    println!(
        "cargo:rustc-env=BENCH_SOURCE_SHA={}",
        short.as_deref().unwrap_or("unknown")
    );

    // A probe that could not run, or that failed, says nothing about the
    // tree: reporting `false` there would let a dirty checkout with an
    // unreadable index pass `check_provenance`. `unknown` is the same
    // honest third state the SHA uses, and the runner reads anything but
    // `false` as dirty.
    let dirty = match Command::new("git")
        .arg("status")
        .arg("--porcelain")
        .current_dir("..")
        .output()
    {
        Ok(o) if o.status.success() => {
            if o.stdout.is_empty() { "false" } else { "true" }
        }
        _ => "unknown",
    };
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
