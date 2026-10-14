//! `bench` CLI (tasks 4, 5, 7 and 11 of the benchmark-harness feature).
//! Flags copied from `features/benchmark-harness/PLAN.md` `## Interface
//! Contracts` (`### Bench — CLI`).

use std::io::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bench::doc;
use bench::gen::{self, Mode};
use bench::matrix::{self, MatrixSpec, RECORDED_QOS, RECORDED_SERVERS, RECORDED_WORKERS};
use bench::record::Window as BenchWindow;
use bench::run_record::{CpuSets, Profile, ServerKind, SyscallMethod};
use bench::runner::{self, HostSetup};
use bench::server::{run_server, ServerArgs, ServerImpl};
use bench::sub::{run_sub, SubArgs};
use clap::{Parser, Subcommand, ValueEnum};

/// The `--profile smoke` default matrix: the single `uring/1/1` row
/// decision Q10 names, so a smoke run needs no matrix flags at all.
/// The recorded profile defaults to `RECORDED_SERVERS`/`RECORDED_WORKERS`/
/// `RECORDED_QOS` instead, the only matrix `check_request` accepts.
const SMOKE_SERVERS: [ServerKind; 1] = [ServerKind::Uring];
const SMOKE_WORKERS: [u32; 1] = [1];
const SMOKE_QOS: [u8; 1] = [1];

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Server {
        #[arg(long = "impl")]
        imp: ServerImplArg,
        #[arg(long)]
        workers: usize,
        #[arg(long)]
        addr: String,
        #[arg(long)]
        connections: u32,
        #[arg(long)]
        report: PathBuf,
    },
    Gen {
        #[arg(long)]
        addr: String,
        #[arg(long = "mode")]
        mode: ModeArg,
        #[arg(long, default_value_t = 0)]
        rate: u64,
        #[arg(long)]
        qos: u8,
        #[arg(long)]
        connections: u32,
        #[arg(long = "qos1-window")]
        qos1_window: u32,
        #[arg(long = "origin-ns")]
        origin_ns: u64,
        #[arg(long = "window-start-ns")]
        window_start_ns: u64,
        #[arg(long = "window-end-ns")]
        window_end_ns: u64,
        #[arg(long = "drain-grace-secs", default_value_t = 0)]
        drain_grace_secs: u64,
        #[arg(long)]
        report: PathBuf,
    },
    Sub {
        #[arg(long)]
        addr: String,
        #[arg(long)]
        qos: u8,
        #[arg(long)]
        connections: u32,
        #[arg(long)]
        report: PathBuf,
    },
    Run {
        #[arg(long = "profile")]
        profile: ProfileArg,
        #[arg(long = "out")]
        out: PathBuf,
        #[arg(long = "servers", value_delimiter = ',')]
        servers: Option<Vec<ServerKindArg>>,
        #[arg(long = "workers", value_delimiter = ',')]
        workers: Option<Vec<u32>>,
        #[arg(long = "qos", value_delimiter = ',')]
        qos: Option<Vec<u8>>,
        #[arg(long = "syscalls")]
        syscalls: SyscallMethodArg,
        #[arg(long = "perf-bin")]
        perf_bin: Option<PathBuf>,
        #[arg(long = "strace-bin")]
        strace_bin: Option<PathBuf>,
        #[arg(long = "mosquitto-bin")]
        mosquitto_bin: Option<PathBuf>,
        #[arg(long = "server-cpus")]
        server_cpus: Option<String>,
        #[arg(long = "generator-cpus")]
        generator_cpus: Option<String>,
        #[arg(long = "resume", default_value_t = false)]
        resume: bool,
    },
    Doc {
        /// Directory of `*.json` records written by `bench run`.
        dir: PathBuf,
        /// Path the rendered `BENCHMARKS.md` document is written to.
        #[arg(long = "out")]
        out: PathBuf,
        /// Render a smoke document without the recorded-profile checks
        /// (task 13's CI gate). Missing or failed configurations render
        /// `FAILED` in their cells instead of refusing the run.
        #[arg(long = "smoke", default_value_t = false)]
        smoke: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ServerImplArg {
    Uring,
    Tokio,
}

impl From<ServerImplArg> for ServerImpl {
    fn from(value: ServerImplArg) -> Self {
        match value {
            ServerImplArg::Uring => ServerImpl::Uring,
            ServerImplArg::Tokio => ServerImpl::Tokio,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ModeArg {
    Throughput,
    Latency,
    Connections,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ProfileArg {
    Recorded,
    Smoke,
}

impl From<ProfileArg> for Profile {
    fn from(value: ProfileArg) -> Self {
        match value {
            ProfileArg::Recorded => Profile::Recorded,
            ProfileArg::Smoke => Profile::Smoke,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ServerKindArg {
    Uring,
    Tokio,
    Mosquitto,
}

impl From<ServerKindArg> for ServerKind {
    fn from(value: ServerKindArg) -> Self {
        match value {
            ServerKindArg::Uring => ServerKind::Uring,
            ServerKindArg::Tokio => ServerKind::Tokio,
            ServerKindArg::Mosquitto => ServerKind::Mosquitto,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SyscallMethodArg {
    Perf,
    Strace,
    None,
}

impl From<SyscallMethodArg> for SyscallMethod {
    fn from(value: SyscallMethodArg) -> Self {
        match value {
            SyscallMethodArg::Perf => SyscallMethod::Perf,
            SyscallMethodArg::Strace => SyscallMethod::Strace,
            SyscallMethodArg::None => SyscallMethod::None,
        }
    }
}

fn main() {
    let cli = Cli::parse();
    let exit_code = match cli.command {
        Command::Server {
            imp,
            workers,
            addr,
            connections,
            report,
        } => dispatch_server(imp, workers, addr, connections, report),
        Command::Gen {
            addr,
            mode,
            rate,
            qos,
            connections,
            qos1_window,
            origin_ns,
            window_start_ns,
            window_end_ns,
            drain_grace_secs,
            report,
        } => dispatch_gen(
            addr,
            mode,
            rate,
            qos,
            connections,
            qos1_window,
            origin_ns,
            window_start_ns,
            window_end_ns,
            drain_grace_secs,
            report,
        ),
        Command::Sub {
            addr,
            qos,
            connections,
            report,
        } => dispatch_sub(addr, qos, connections, report),
        Command::Run {
            profile,
            out,
            servers,
            workers,
            qos,
            syscalls,
            perf_bin,
            strace_bin,
            mosquitto_bin,
            server_cpus,
            generator_cpus,
            resume,
        } => dispatch_run(
            profile,
            out,
            servers,
            workers,
            qos,
            syscalls,
            perf_bin,
            strace_bin,
            mosquitto_bin,
            server_cpus,
            generator_cpus,
            resume,
        ),
        Command::Doc { dir, out, smoke } => dispatch_doc(dir, out, smoke),
    };
    std::process::exit(exit_code);
}

fn dispatch_server(
    imp: ServerImplArg,
    workers: usize,
    addr: String,
    connections: u32,
    report: PathBuf,
) -> i32 {
    match run_server(&ServerArgs {
        imp: imp.into(),
        workers,
        addr,
        connections,
        report,
    }) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{e}");
            1
        }
    }
}

fn dispatch_gen(
    addr: String,
    mode: ModeArg,
    rate: u64,
    qos: u8,
    connections: u32,
    qos1_window: u32,
    origin_ns: u64,
    window_start_ns: u64,
    window_end_ns: u64,
    drain_grace_secs: u64,
    report: PathBuf,
) -> i32 {
    let result: Result<(), String> = (|| -> Result<(), String> {
        let addr: SocketAddr = addr.parse().map_err(|e| format!("addr: {e}"))?;
        let mode = match mode {
            ModeArg::Throughput => Mode::Throughput,
            ModeArg::Connections => Mode::Connections,
            ModeArg::Latency => Mode::Latency { rate },
        };
        let config = gen::GenConfig {
            addr,
            mode,
            qos,
            connections,
            qos1_window,
            origin_ns,
            window: BenchWindow {
                start_ns: window_start_ns,
                end_ns: window_end_ns,
            },
            drain_grace: Duration::from_secs(drain_grace_secs),
        };
        let report_path = report;
        if let Some(parent) = report_path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("create report dir: {e}"))?;
            }
        }
        let gen_report = gen::run_generator(&config)?;
        let file = std::fs::File::create(&report_path)
            .map_err(|e| format!("create report file: {e}"))?;
        serde_json::to_writer_pretty(&file, &gen_report)
            .map_err(|e| format!("write report: {e}"))?;
        Ok(())
    })();
    match result {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{e}");
            1
        }
    }
}

fn dispatch_sub(addr: String, qos: u8, connections: u32, report: PathBuf) -> i32 {
    match run_sub(&SubArgs {
        addr,
        qos,
        connections,
        report,
    }) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{e}");
            1
        }
    }
}

fn dispatch_run(
    profile: ProfileArg,
    out: PathBuf,
    servers: Option<Vec<ServerKindArg>>,
    workers: Option<Vec<u32>>,
    qos: Option<Vec<u8>>,
    syscalls: SyscallMethodArg,
    perf_bin: Option<PathBuf>,
    strace_bin: Option<PathBuf>,
    mosquitto_bin: Option<PathBuf>,
    server_cpus: Option<String>,
    generator_cpus: Option<String>,
    resume: bool,
) -> i32 {
    let profile: Profile = profile.into();
    let syscalls: SyscallMethod = syscalls.into();
    // Defaults follow the profile: a smoke run that omits these flags
    // gets the one-row smoke subset, a recorded run gets the fixed §9.1
    // matrix its own full-matrix check requires.
    let (servers_default, workers_default, qos_default) = match profile {
        Profile::Recorded => (
            RECORDED_SERVERS.to_vec(),
            RECORDED_WORKERS.to_vec(),
            RECORDED_QOS.to_vec(),
        ),
        Profile::Smoke => (
            SMOKE_SERVERS.to_vec(),
            SMOKE_WORKERS.to_vec(),
            SMOKE_QOS.to_vec(),
        ),
    };
    let servers: Vec<ServerKind> = servers
        .map_or(servers_default, |v| v.into_iter().map(ServerKind::from).collect());
    let workers: Vec<u32> = workers.unwrap_or(workers_default);
    let qos: Vec<u8> = qos.unwrap_or(qos_default);

    let spec = MatrixSpec {
        profile,
        servers,
        workers,
        qos,
        out_dir: out.clone(),
        resume,
    };

    // Phase 1: refusal checks. No process is spawned until these pass.
    if let Err(e) = matrix::check_request(
        &spec,
        syscalls,
        perf_bin.as_deref(),
        strace_bin.as_deref(),
        mosquitto_bin.as_deref(),
    ) {
        eprintln!("{e}");
        return 1;
    }

    // Phase 2: ctrlc handler, before the first subprocess. With the
    // `termination` feature the handler covers SIGTERM as well; SIGINT is
    // always handled. The provenance and environment phases below spawn
    // children (`git rev-parse`, one `{tool} --version` probe per supplied
    // tool), and until the handler is installed SIGTERM takes its default
    // action: the runner dies and the probe it started keeps running with
    // no diagnostic (AC-52). With the handler installed the runner stays
    // alive, so `runner::tool_version`'s kill-and-reap always runs — at
    // the latest after its own `TOOL_VERSION_TIMEOUT_SECS` deadline — and
    // the flag is read again before the matrix starts.
    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_for_signal = cancel.clone();
    if let Err(e) = ctrlc::set_handler(move || {
        cancel_for_signal.store(true, Ordering::SeqCst);
    }) {
        eprintln!("install ctrl-c handler: {e}");
        return 1;
    }

    // Phase 3: recorded-profile provenance. A recorded run must come
    // from a clean binary that matches the working tree (AC-57).
    if profile == Profile::Recorded {
        let head_sha = match head_sha() {
            Ok(s) => s,
            Err(e) => {
                eprintln!("{e}");
                return 1;
            }
        };
        // `build.rs` emits `unknown` when its `git status` probe could not
        // run: cleanliness is unproven, not proven false. Only the literal
        // `false` is clean, which is also how `runner::capture_env` reads
        // the same variable.
        let build_dirty = env!("BENCH_SOURCE_DIRTY") != "false";
        let build_sha = env!("BENCH_SOURCE_SHA");
        if let Err(e) = runner::check_provenance(build_sha, build_dirty, &head_sha) {
            eprintln!("{e}");
            return 1;
        }
    }

    // Phase 4: build HostSetup.
    let bench_exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("current_exe: {e}");
            return 1;
        }
    };
    let syscalls_runner: SyscallMethod = syscalls;
    let tools: Vec<PathBuf> = [
        perf_bin.clone(),
        strace_bin.clone(),
        mosquitto_bin.clone(),
    ]
    .into_iter()
    .flatten()
    .collect();
    let env = match runner::capture_env(&tools) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("{e}");
            return 1;
        }
    };
    let cpu_sets = CpuSets {
        server: server_cpus.unwrap_or_else(|| runner::DEFAULT_SERVER_CPUS.to_string()),
        generator: generator_cpus.unwrap_or_else(|| runner::DEFAULT_GENERATOR_CPUS.to_string()),
    };
    let scratch_dir = out.join(".scratch");
    let host = HostSetup {
        bench_exe,
        mosquitto_bin: mosquitto_bin.clone(),
        syscalls: syscalls_runner,
        perf_bin,
        strace_bin,
        cpu_sets,
        env,
        scratch_dir,
        cancel: cancel.clone(),
    };

    // Phase 5: --resume env check (AC-63).
    if resume {
        let existing = match matrix::read_records(&out) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("read records: {e}");
                return 1;
            }
        };
        if let Err(e) = matrix::check_resume(
            &existing,
            profile,
            host.syscalls,
            &host.env,
            &host.cpu_sets,
        ) {
            eprintln!("{e}");
            return 1;
        }
    }

    // Phase 6: run the matrix. A signal that arrived during the preflight
    // starts nothing: `run_matrix` would report the same cancellation on
    // its first configuration.
    if cancel.load(Ordering::SeqCst) {
        eprintln!("cancelled");
        return 1;
    }
    match matrix::run_matrix(&spec, &host) {
        Ok(outcome) => {
            if outcome.failed.is_empty() {
                0
            } else {
                // Name the failed configurations: the ones a throughput
                // failure skipped have no record of their own, so the exit
                // code is the operator's only sign that they exist.
                for key in &outcome.failed {
                    eprintln!("failed: {key:?}");
                }
                2
            }
        }
        Err(e) => {
            eprintln!("{e}");
            1
        }
    }
}

/// Render `BENCHMARKS.md` for the records in `dir`. `smoke` skips the
/// recorded-profile checks (task 13's CI gate): missing or failed
/// configurations render `FAILED` in their cells instead of refusing the
/// run. The single `Err` is the operator's only sign that the record
/// set is unpublishable, so it is printed verbatim and the process
/// exits 1 (AC-47).
fn dispatch_doc(dir: PathBuf, out: PathBuf, smoke: bool) -> i32 {
    let result: Result<(), String> = (|| -> Result<(), String> {
        // The guard resolves `--out` through its parent directory, so
        // the parent is created first: a path whose parent does not
        // exist yet resolves to nothing, and `..` behind a missing
        // component then hides the record the write would land on.
        if let Some(parent) = out.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("create out dir: {e}"))?;
            }
        }
        check_out_not_a_record(&dir, &out)?;
        let records = doc::load_records(&dir).map_err(|e| e.clone())?;
        let body = doc::render(&records, smoke).map_err(|e| format!("{e:?}"))?;
        write_atomically(&out, &body)?;
        Ok(())
    })();
    match result {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{e}");
            1
        }
    }
}

/// Suffix of the temporary file [`write_atomically`] renames over the
/// destination. The name in front of it carries the process id and a
/// nanosecond stamp, so the temporary path is never one an outside
/// writer can plant in advance.
const DOC_TMP_SUFFIX: &str = ".tmp";

/// Extension of the records `dir` holds, and the one an `--out` path
/// inside it must not carry.
const RECORD_EXTENSION: &str = "json";

/// Refuse an `--out` path that is, or would become, one of the `*.json`
/// records in `dir`.
///
/// An `--out` naming an existing record is read by `load_records` and
/// the write then replaces the raw measurement with Markdown while the
/// command exits 0. Both sides are canonicalized, so a relative path or
/// a symlink naming the same record is caught as well.
///
/// A `*.json` `--out` that does not exist yet passes that test and
/// still corrupts the directory: the Markdown lands beside the records
/// and the next `bench doc` or `bench run --resume` fails parsing it as
/// a `RunRecord`. Such a path is refused by extension once its parent
/// resolves to `dir`.
///
/// Both tests read the same resolved path, and they resolve it the way
/// the write does: through the canonical parent directory, never
/// through `out` alone. A path that does not exist yet cannot be
/// canonicalized, and a `..` component behind a directory the run is
/// about to create — `<dir>/new/../record.json` — resolves to nothing
/// while the write still lands on `<dir>/record.json`. The caller
/// creates the parent before calling this, so the parent resolves here
/// for every path the write can reach.
fn check_out_not_a_record(dir: &Path, out: &Path) -> Result<(), String> {
    let dir_real = dir
        .canonicalize()
        .map_err(|e| format!("read dir {}: {e}", dir.display()))?;
    let Some(name) = out.file_name() else {
        return Err(format!("out path {} names no file", out.display()));
    };
    let parent = out.parent().filter(|p| !p.as_os_str().is_empty());
    let parent_real = match parent {
        Some(p) => p
            .canonicalize()
            .map_err(|e| format!("read out dir {}: {e}", p.display()))?,
        None => std::env::current_dir()
            .map_err(|e| format!("read the current directory: {e}"))?,
    };
    // An existing `out` is canonicalized whole, so an `--out` symlink
    // pointing at a record elsewhere is resolved to that record; one
    // that does not exist yet is the file the write creates inside the
    // resolved parent.
    let out_real = out
        .canonicalize()
        .unwrap_or_else(|_| parent_real.join(name));
    let entries =
        std::fs::read_dir(dir).map_err(|e| format!("read dir {}: {e}", dir.display()))?;
    for entry in entries {
        let path = entry.map_err(|e| format!("entry: {e}"))?.path();
        if path.extension().and_then(|e| e.to_str()) != Some(RECORD_EXTENSION) {
            continue;
        }
        let Ok(record_real) = path.canonicalize() else {
            continue;
        };
        if record_real == out_real {
            return Err(format!(
                "out path {} is a run record in {}: refusing to overwrite a raw measurement",
                out.display(),
                dir.display()
            ));
        }
    }
    if out_real.extension().and_then(|e| e.to_str()) == Some(RECORD_EXTENSION)
        && out_real.parent() == Some(dir_real.as_path())
    {
        return Err(format!(
            "out path {} is a `*.{RECORD_EXTENSION}` file in the record directory {}: \
             refusing to write a document a later run reads as a raw measurement",
            out.display(),
            dir.display()
        ));
    }
    Ok(())
}

/// Write `body` to `path` through a temporary file in the same
/// directory and rename it into place, so a failing write cannot leave
/// a truncated or partial document where a complete one stood.
///
/// The temporary file is created with `create_new` (`O_CREAT | O_EXCL`),
/// which neither follows a symlink nor truncates an existing file: a
/// `*.tmp` path planted ahead of the run — a symlink to one of the raw
/// records, say — ends the write with an error instead of destroying
/// the measurement behind it. The unique name keeps two runs in one
/// directory from colliding on that path.
fn write_atomically(path: &Path, body: &str) -> Result<(), String> {
    let Some(name) = path.file_name() else {
        return Err(format!("write out: {} names no file", path.display()));
    };
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let mut tmp_name = name.to_os_string();
    tmp_name.push(format!(".{}.{stamp}{DOC_TMP_SUFFIX}", std::process::id()));
    let tmp = path.with_file_name(tmp_name);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .map_err(|e| format!("write out: create {}: {e}", tmp.display()))?;
    if let Err(e) = file.write_all(body.as_bytes()) {
        drop(file);
        let _ = std::fs::remove_file(&tmp);
        return Err(format!("write out: {e}"));
    }
    drop(file);
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(format!("write out: {e}"));
    }
    Ok(())
}

/// `git rev-parse --short HEAD` of the working tree the runner is
/// about to invoke, for [`runner::check_provenance`].
fn head_sha() -> Result<String, String> {
    let out = std::process::Command::new("git")
        .arg("rev-parse")
        .arg("--short")
        .arg("HEAD")
        .output()
        .map_err(|e| format!("git rev-parse: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "git rev-parse exited with {:?}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

