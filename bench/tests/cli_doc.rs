//! CLI tests for `bench doc` (the document half of task 12/13 of the
//! benchmark-harness feature): the subcommand renders a smoke document
//! from the records of a directory (AC-50), refuses a smoke record in a
//! recorded document (AC-47) and refuses an `--out` path that is, or
//! would become, one of the raw records it reads.
//!
//! The records are written as fixtures instead of measured: the
//! attended smoke matrix is covered by `cli_run.rs`, and this file
//! tests the CLI wiring around `doc::load_records` / `doc::render`.

// The fixture's stored metrics are built to equal what `analysis::derive`
// computes from its raw timeline, which needs the same `f64` conversions
// the runner makes when it fills a record.
#![allow(clippy::cast_precision_loss)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use bench::record::{Timeline, Window};
use bench::run_record::{
    Build, Counts, CpuSets, CpuUsage, DriverFds, Env, GeneratorErrors, Profile, RunKind, RunRecord,
    ServerKind, SyscallMethod, Syscalls, SCHEMA_VERSION,
};
use bench::runner::ProtocolConstants;

const ONE_SECOND_NS: u64 = 1_000_000_000;
/// Whole-second origin, as reconciliation row 26 requires.
const ORIGIN_NS: u64 = 1_700_000_000_000_000_000;
/// Deliveries per second of every window second of the fixture.
const RATE_PER_SEC: u64 = 100;
const CONNECTIONS: u32 = 100;
const QOS1_WINDOW: u32 = 32;
const PAYLOAD_BYTES: u32 = 64;
const CLK_TCK: u64 = 100;
const SERVER_TICKS: u64 = 1900;
const GENERATOR_TICKS: u64 = 2000;
const STARTED_AT_UNIX: u64 = 1_791_273_600;
const SMOKE_HEADER: &str = "SMOKE — not publishable";

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bench-cli-doc-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    ));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// A valid smoke uring throughput record: a uniform [`RATE_PER_SEC`]
/// over every second of the smoke window, with every stored metric
/// equal to what `analysis::derive` reads off that timeline, so
/// `doc::render` accepts it through `recompute`.
fn smoke_throughput_record(repetition: u32) -> RunRecord {
    let constants = ProtocolConstants::for_profile(Profile::Smoke);
    let window = Window {
        start_ns: ORIGIN_NS + constants.warmup_secs * ONE_SECOND_NS,
        end_ns: ORIGIN_NS + (constants.warmup_secs + constants.window_secs) * ONE_SECOND_NS,
    };
    let delivered = RATE_PER_SEC * constants.window_secs;
    let timeline = Timeline {
        origin_sec: window.start_ns / ONE_SECOND_NS,
        counts: vec![
            RATE_PER_SEC;
            usize::try_from(constants.window_secs).expect("window fits in usize")
        ],
    };
    let rate = delivered as f64 / constants.window_secs as f64;
    RunRecord {
        schema: SCHEMA_VERSION,
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        offered_rate: None,
        repetition,
        attempt: 1,
        connections: CONNECTIONS,
        payload_bytes: PAYLOAD_BYTES,
        qos1_window: QOS1_WINDOW,
        warmup_secs: constants.warmup_secs,
        window_secs: constants.window_secs,
        drain_grace_secs: constants.drain_grace_secs,
        build: Build::Release,
        origin_ns: ORIGIN_NS,
        window,
        timeline,
        cycles_in_window: None,
        counts: Counts {
            scheduled: None,
            window_scheduled: None,
            offered: delivered,
            acked: Some(delivered),
            delivered,
            dropped: Some(0),
            unresolved: 0,
            duplicates: 0,
            foreign: 0,
            latency_over_range: 0,
            clock_anomalies: 0,
        },
        throughput_msgs_per_sec: Some(rate),
        last10s_msgs_per_sec: Some(rate),
        latency_ns: None,
        lateness: Vec::new(),
        lateness_p99_ns: None,
        last_write_start_ns: None,
        connections_per_sec: None,
        generator_errors: GeneratorErrors {
            connect: 0,
            protocol: 0,
            missed_slots: 0,
            stalled: 0,
            lateness_over_range: 0,
        },
        syscalls: Syscalls {
            method: SyscallMethod::None,
            count: None,
            per_msg: None,
            raw: None,
            enabled_ns: None,
            disabled_ns: None,
            exit: None,
            diagnostics: None,
        },
        driver: Some(DriverFds {
            io_uring_fds: 1,
            eventpoll_fds: 0,
        }),
        cpu_sets: CpuSets {
            server: bench::runner::DEFAULT_SERVER_CPUS.to_string(),
            generator: bench::runner::DEFAULT_GENERATOR_CPUS.to_string(),
        },
        env: Env {
            kernel: "6.18.33.2-microsoft-standard-WSL2".to_string(),
            cpu_model: "Intel(R) Core(TM) i7-14650HX".to_string(),
            host: "test-host".to_string(),
            git_sha: "b219eb1".to_string(),
            git_dirty: false,
            tools: Vec::new(),
        },
        cpu: Some(CpuUsage {
            clk_tck: CLK_TCK,
            sampled_ns: constants.window_secs * ONE_SECOND_NS,
            server_ticks: SERVER_TICKS,
            generator_ticks: GENERATOR_TICKS,
        }),
        host_cpu_pressure: None,
        started_at_unix: STARTED_AT_UNIX,
        harness_errors: Vec::new(),
        valid: true,
        invalid_reasons: Vec::new(),
        cancelled: false,
    }
}

/// Write one record per smoke repetition into `dir` under the file name
/// `bench run` gives it, and return those paths.
fn write_smoke_records(dir: &Path) -> Vec<PathBuf> {
    let constants = ProtocolConstants::for_profile(Profile::Smoke);
    (1..=constants.repetitions)
        .map(|rep| {
            let record = smoke_throughput_record(rep);
            let path = dir.join(record.file_name());
            let json = serde_json::to_string_pretty(&record).expect("serialize record");
            fs::write(&path, json).expect("write record");
            path
        })
        .collect()
}

/// Run `bench doc` with `args` and return its exit code and stderr.
fn run_doc(args: &[&str]) -> (Option<i32>, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_bench"))
        .arg("doc")
        .args(args)
        .output()
        .expect("spawn bench doc");
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    (output.status.code(), stderr)
}

#[test]
fn doc_writes_a_smoke_document_from_a_record_directory() {
    let work = temp_dir("smoke-ok");
    let records_dir = work.join("records");
    fs::create_dir_all(&records_dir).expect("create records dir");
    write_smoke_records(&records_dir);
    // A parent that does not exist yet: `bench doc` creates it.
    let out = work.join("generated").join("BENCHMARKS.md");

    let (code, stderr) = run_doc(&[
        records_dir.to_str().expect("utf-8 dir"),
        "--out",
        out.to_str().expect("utf-8 out"),
        "--smoke",
    ]);
    assert_eq!(code, Some(0), "bench doc --smoke must exit 0: {stderr}");
    let body = fs::read_to_string(&out).expect("read document");
    assert!(
        body.contains(SMOKE_HEADER),
        "smoke document must carry the smoke header:\n{body}"
    );
    assert!(
        body.contains("uring"),
        "smoke document must name the measured server:\n{body}"
    );
}

#[test]
fn doc_refuses_a_smoke_record_without_the_smoke_flag() {
    let work = temp_dir("smoke-in-recorded");
    let records_dir = work.join("records");
    fs::create_dir_all(&records_dir).expect("create records dir");
    write_smoke_records(&records_dir);
    let out = work.join("BENCHMARKS.md");

    let (code, stderr) = run_doc(&[
        records_dir.to_str().expect("utf-8 dir"),
        "--out",
        out.to_str().expect("utf-8 out"),
    ]);
    assert_eq!(code, Some(1), "a smoke record must refuse a recorded doc");
    assert!(
        stderr.contains("SmokeRecordInRecordedDoc"),
        "stderr must name the DocError, got: {stderr}"
    );
    assert!(
        !out.exists(),
        "a refused render must write no document at {}",
        out.display()
    );
}

#[test]
fn doc_refuses_an_empty_record_directory() {
    let work = temp_dir("no-records");
    let records_dir = work.join("records");
    fs::create_dir_all(&records_dir).expect("create records dir");
    let out = work.join("BENCHMARKS.md");

    let (code, stderr) = run_doc(&[
        records_dir.to_str().expect("utf-8 dir"),
        "--out",
        out.to_str().expect("utf-8 out"),
        "--smoke",
    ]);
    assert_eq!(code, Some(1), "an empty directory must exit 1");
    assert!(
        stderr.contains("NoRecords"),
        "stderr must name the DocError, got: {stderr}"
    );
    assert!(!out.exists(), "no document on refusal");
}

#[test]
fn doc_refuses_an_out_path_that_is_one_of_the_records() {
    let work = temp_dir("out-is-record");
    let records_dir = work.join("records");
    fs::create_dir_all(&records_dir).expect("create records dir");
    let records = write_smoke_records(&records_dir);
    let target = records.first().expect("one record").clone();
    let before = fs::read_to_string(&target).expect("read record");

    let (code, stderr) = run_doc(&[
        records_dir.to_str().expect("utf-8 dir"),
        "--out",
        target.to_str().expect("utf-8 out"),
        "--smoke",
    ]);
    assert_eq!(code, Some(1), "overwriting a raw record must exit 1");
    assert!(
        stderr.contains("refusing"),
        "stderr must say it refuses the overwrite, got: {stderr}"
    );
    assert_eq!(
        fs::read_to_string(&target).expect("read record"),
        before,
        "the raw record must be untouched"
    );
}

#[test]
fn doc_refuses_a_new_json_out_path_inside_the_record_directory() {
    let work = temp_dir("out-json-in-dir");
    let records_dir = work.join("records");
    fs::create_dir_all(&records_dir).expect("create records dir");
    write_smoke_records(&records_dir);
    // A `*.json` path no record holds yet: a document written there is
    // read as a raw measurement by the next `bench doc` or `--resume`.
    let out = records_dir.join("document.json");

    let (code, stderr) = run_doc(&[
        records_dir.to_str().expect("utf-8 dir"),
        "--out",
        out.to_str().expect("utf-8 out"),
        "--smoke",
    ]);
    assert_eq!(code, Some(1), "a .json out path in the record dir exits 1");
    assert!(
        stderr.contains("refusing"),
        "stderr must say it refuses the path, got: {stderr}"
    );
    assert!(!out.exists(), "no document beside the records");
}
