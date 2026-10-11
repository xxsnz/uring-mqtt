//! Integration tests for `bench::runner` (task 9 of the benchmark-harness
//! feature). The smoke harness exercises `run_one` end to end against the
//! real `bench` binary; the script-based tests use a `tempfile`-style
//! shell script as `bench_exe` to test the no-reports and slow-startup
//! paths; the unit tests cover `check_provenance` and `mosquitto_conf`.

use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use bench::record::DeliveryReport;
use bench::run_record::{
    Build, Counts, CpuSets, CpuUsage, DriverFds, Env, GeneratorErrors, Profile, RunKind, RunRecord,
    ServerKind, SyscallMethod, Syscalls, SCHEMA_VERSION,
};
use bench::record::Timeline;
use bench::record::Window;

use bench::runner::{
    capture_env, check_provenance, mosquitto_conf, run_one, HostSetup, RunSpec,
};

/// Build a HostSetup pointing at the real bench binary with default CPU
/// sets, an empty tool list, and the supplied scratch_dir/cancel flag.
fn real_host_setup(scratch_dir: PathBuf, cancel: Arc<AtomicBool>) -> HostSetup {
    HostSetup {
        bench_exe: PathBuf::from(env!("CARGO_BIN_EXE_bench")),
        mosquitto_bin: None,
        syscalls: SyscallMethod::None,
        perf_bin: None,
        strace_bin: None,
        cpu_sets: CpuSets {
            server: bench::runner::DEFAULT_SERVER_CPUS.to_string(),
            generator: bench::runner::DEFAULT_GENERATOR_CPUS.to_string(),
        },
        env: capture_env(&[]).expect("capture_env"),
        scratch_dir,
        cancel,
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bench-runner-{}-{}-{}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    ));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn make_script(dir: &std::path::Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    let mut f = fs::File::create(&path).expect("create script");
    f.write_all(b"#!/bin/sh\n").expect("write shebang");
    f.write_all(body.as_bytes()).expect("write body");
    let mut perms = fs::metadata(&path).expect("stat").permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    {
        use std::os::unix::fs::PermissionsExt;
        perms.set_mode(0o755);
    }
    fs::set_permissions(&path, perms).expect("chmod");
    path
}

#[test]
fn smoke_uring_throughput_run_is_valid_with_driver_proof() {
    let dir = temp_dir("smoke-uring-thr");
    let host = real_host_setup(dir.clone(), Arc::new(AtomicBool::new(false)));
    let spec = RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        rate_fraction_pct: None,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
    };

    let record = run_one(&spec, &host);

    assert!(record.valid, "smoke uring throughput must validate; reasons: {:?}", record.invalid_reasons);
    assert_eq!(
        record.driver,
        Some(DriverFds {
            io_uring_fds: 1,
            eventpoll_fds: 0
        }),
        "1-worker uring endpoint must show one io_uring fd and zero eventpoll fds"
    );
    assert!(record.counts.offered > 0, "smoke must publish some messages");
    assert_eq!(
        record.counts.delivered, record.counts.offered,
        "delivered must equal offered"
    );
    let cpu = record.cpu.expect("cpu ticks present");
    assert!(cpu.server_ticks > 0, "server ticks must be > 0");
    assert!(cpu.generator_ticks > 0, "generator ticks must be > 0");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn smoke_tokio_throughput_run_is_valid() {
    let dir = temp_dir("smoke-tokio-thr");
    let host = real_host_setup(dir.clone(), Arc::new(AtomicBool::new(false)));
    let spec = RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Tokio,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        rate_fraction_pct: None,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
    };
    let record = run_one(&spec, &host);

    assert!(record.valid, "smoke tokio throughput must validate; reasons: {:?}", record.invalid_reasons);
    assert_eq!(record.counts.delivered, record.counts.offered);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn smoke_uring_connections_run_is_valid() {
    let dir = temp_dir("smoke-uring-conn");
    let cancel = Arc::new(AtomicBool::new(false));
    let host = real_host_setup(dir.clone(), cancel);
    let spec = RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: None,
        kind: RunKind::Connections,
        rate_fraction_pct: None,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
    };
    let record = run_one(&spec, &host);

    assert!(
        record.valid,
        "smoke uring connections must be valid; invalid_reasons: {:?}, harness_errors: {:?}",
        record.invalid_reasons, record.harness_errors
    );
    assert!(
        record.cycles_in_window.unwrap_or(0) > 0,
        "connections mode must complete cycles"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn run_one_invalidates_a_run_whose_children_write_no_reports() {
    let dir = temp_dir("no-reports");
    let cancel = Arc::new(AtomicBool::new(false));

    // A fake `bench`: every invocation prints READY, reads ARM, prints
    // ARMED, and exits 0. It never writes the JSON reports the runner
    // expects, so the run must end with `missing gen report` and any
    // other no-report harness errors in `invalid_reasons`.
    let script = make_script(&dir, "fake-bench.sh", "echo READY\nread line\necho ARMED\nexit 0\n");

    let host = HostSetup {
        bench_exe: script.clone(),
        mosquitto_bin: None,
        syscalls: SyscallMethod::None,
        perf_bin: None,
        strace_bin: None,
        cpu_sets: CpuSets {
            server: bench::runner::DEFAULT_SERVER_CPUS.to_string(),
            generator: bench::runner::DEFAULT_GENERATOR_CPUS.to_string(),
        },
        env: capture_env(&[]).expect("capture_env"),
        scratch_dir: dir.clone(),
        cancel,
    };
    let spec = RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        rate_fraction_pct: None,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
    };

    let record = run_one(&spec, &host);

    assert!(!record.valid, "no-reports run must be invalid");
    assert!(
        record
            .invalid_reasons
            .iter()
            .any(|r| r.contains("missing gen report")),
        "expected 'missing gen report' in {:?}",
        record.invalid_reasons
    );

    // No `fake-bench.sh` process remains after run_one returns. Use
    // `pgrep -f` against the resolved script path so the search matches
    // only our child processes.
    let lookup = std::process::Command::new("pgrep")
        .arg("-f")
        .arg(script.to_string_lossy().as_ref())
        .output();
    if let Ok(out) = lookup {
        assert!(
            out.stdout.is_empty(),
            "expected no fake-bench processes, found: {:?}",
            String::from_utf8_lossy(&out.stdout)
        );
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn run_one_succeeds_when_the_endpoint_starts_slower_than_the_lead() {
    let dir = temp_dir("slow-startup");
    let real_bench = env!("CARGO_BIN_EXE_bench");
    // Sleeping 3 s for `server` is the slow-startup case; the `gen`
    // invocation must short-circuit straight to the real bench binary
    // so the generator actually runs and writes gen.json.
    let body = format!(
        "if [ \"$1\" = server ]; then sleep 3; fi\nexec {real_bench} \"$@\"\n"
    );
    let script = make_script(&dir, "slow-bench.sh", &body);

    let host = HostSetup {
        bench_exe: script,
        mosquitto_bin: None,
        syscalls: SyscallMethod::None,
        perf_bin: None,
        strace_bin: None,
        cpu_sets: CpuSets {
            server: bench::runner::DEFAULT_SERVER_CPUS.to_string(),
            generator: bench::runner::DEFAULT_GENERATOR_CPUS.to_string(),
        },
        env: capture_env(&[]).expect("capture_env"),
        scratch_dir: dir.clone(),
        cancel: Arc::new(AtomicBool::new(false)),
    };
    let spec = RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        rate_fraction_pct: None,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
    };

    let record = run_one(&spec, &host);

    assert!(
        record.valid,
        "slow-startup run must validate; reasons: {:?}",
        record.invalid_reasons
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn provenance_check_refuses_a_stale_or_dirty_build() {
    // Stale: build SHA differs from the checkout.
    let err = check_provenance("abc1234", false, "def5678")
        .expect_err("stale build must be rejected");
    assert!(err.contains("rebuild"), "unexpected error: {err}");

    // Dirty: build_dirty=true.
    let err = check_provenance("abc1234", true, "abc1234")
        .expect_err("dirty build must be rejected");
    assert!(err.contains("dirty"), "unexpected error: {err}");

    // Matching and clean: Ok.
    check_provenance("abc1234", false, "abc1234").expect("matching build must pass");
}

#[test]
fn mosquitto_conf_renders_the_approved_options() {
    let conf = mosquitto_conf(1883);
    let expected = "listener 1883 127.0.0.1\n\
                    allow_anonymous true\n\
                    persistence false\n\
                    max_queued_messages 0\n\
                    max_inflight_messages 0\n\
                    set_tcp_nodelay true\n\
                    log_dest none\n";
    assert_eq!(conf, expected);
}

// `DeliveryReport` and `RunRecord` are imported so this file's
// `use` block surfaces the public types the runner consumes; clippy
// trips if the imports go unused.
#[allow(dead_code)]
fn _shape_references() {
    let _ = SCHEMA_VERSION;
    let _ = Build::Release;
    let _ = Counts {
        scheduled: None,
        window_scheduled: None,
        offered: 0,
        acked: None,
        delivered: 0,
        dropped: None,
        unresolved: 0,
        duplicates: 0,
        foreign: 0,
        latency_over_range: 0,
        clock_anomalies: 0,
    };
    let _ = GeneratorErrors {
        connect: 0,
        protocol: 0,
        missed_slots: 0,
        stalled: 0,
        lateness_over_range: 0,
    };
    let _ = Syscalls {
        method: SyscallMethod::None,
        count: None,
        per_msg: None,
        raw: None,
        enabled_ns: None,
        disabled_ns: None,
        exit: None,
        diagnostics: None,
    };
    let _ = Timeline {
        origin_sec: 0,
        counts: Vec::new(),
    };
    let _ = Window {
        start_ns: 0,
        end_ns: 0,
    };
    let _ = Env {
        kernel: String::new(),
        cpu_model: String::new(),
        host: String::new(),
        git_sha: String::new(),
        git_dirty: false,
        tools: Vec::new(),
    };
    let _ = CpuUsage {
        clk_tck: 0,
        sampled_ns: 0,
        server_ticks: 0,
        generator_ticks: 0,
    };
    let _ = RunRecord {
        schema: SCHEMA_VERSION,
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        rate_fraction_pct: None,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
        connections: 0,
        payload_bytes: 0,
        qos1_window: 0,
        warmup_secs: 0,
        window_secs: 0,
        drain_grace_secs: 0,
        build: Build::Release,
        origin_ns: 0,
        window: Window {
            start_ns: 0,
            end_ns: 0,
        },
        timeline: Timeline {
            origin_sec: 0,
            counts: Vec::new(),
        },
        cycles_in_window: None,
        counts: Counts {
            scheduled: None,
            window_scheduled: None,
            offered: 0,
            acked: None,
            delivered: 0,
            dropped: None,
            unresolved: 0,
            duplicates: 0,
            foreign: 0,
            latency_over_range: 0,
            clock_anomalies: 0,
        },
        throughput_msgs_per_sec: None,
        last10s_msgs_per_sec: None,
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
        driver: None,
        cpu_sets: CpuSets {
            server: String::new(),
            generator: String::new(),
        },
        env: Env {
            kernel: String::new(),
            cpu_model: String::new(),
            host: String::new(),
            git_sha: String::new(),
            git_dirty: false,
            tools: Vec::new(),
        },
        cpu: None,
        started_at_unix: 0,
        harness_errors: Vec::new(),
        valid: false,
        invalid_reasons: Vec::new(),
    };
    let _ = DeliveryReport {
        delivered: 0,
        duplicates: 0,
        foreign: 0,
        latency_over_range: 0,
        clock_anomalies: 0,
        dropped: None,
        timeline: Timeline {
            origin_sec: 0,
            counts: Vec::new(),
        },
        latency: Vec::new(),
    };
}
