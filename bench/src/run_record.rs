//! One typed JSON record per benchmark run. The schema is shared by the
//! runner (which fills it from raw observations) and `bench doc` (which
//! recomputes every derived field on load). `key()` groups records by the
//! configuration that produced them and `file_name()` is the on-disk
//! convention used by `bench run` and `bench doc`.

use serde::{Deserialize, Serialize};

use crate::record::{Timeline, Window, WorkerLoad};

pub const SCHEMA_VERSION: u32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Profile {
    Recorded,
    Smoke,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServerKind {
    Uring,
    Tokio,
    Mosquitto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunKind {
    Throughput,
    Latency,
    Connections,
    /// The counted run (spec §9.1): open-loop at the higher declared rate
    /// with the syscall counter attached; it supplies only `syscalls/msg`.
    Counted,
}

impl RunKind {
    /// `Latency` and `Counted`: the kinds the generator paces open-loop
    /// from a slot grid, so every open-loop reconciliation row applies.
    pub fn is_open_loop(self) -> bool {
        matches!(self, RunKind::Latency | RunKind::Counted)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyscallMethod {
    Perf,
    Strace,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Build {
    Release,
    Debug,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counts {
    pub scheduled: Option<u64>,
    pub window_scheduled: Option<u64>,
    pub offered: u64,
    pub acked: Option<u64>,
    pub delivered: u64,
    pub dropped: Option<u64>,
    pub unresolved: u64,
    pub duplicates: u64,
    pub foreign: u64,
    pub latency_over_range: u64,
    pub clock_anomalies: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencySummary {
    pub p50: u64,
    pub p99: u64,
    pub max: u64,
    pub histogram: Vec<(u64, u64)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CounterExit {
    Code(i32),
    Signal(i32),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Syscalls {
    pub method: SyscallMethod,
    pub count: Option<u64>,
    pub per_msg: Option<f64>,
    pub raw: Option<String>,
    pub enabled_ns: Option<u64>,
    pub disabled_ns: Option<u64>,
    pub exit: Option<CounterExit>,
    pub diagnostics: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DriverFds {
    pub io_uring_fds: u32,
    pub eventpoll_fds: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CpuSets {
    pub server: String,
    pub generator: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CpuUsage {
    pub clk_tck: u64,
    /// Monotonic nanoseconds between the start and the end tick sample.
    /// The ticks are consumed over this interval, which the runner takes as
    /// tight to the window boundaries as it can but cannot pin to them, so
    /// `analysis::cpu_percent` divides by this and not by `window_secs`.
    pub sampled_ns: u64,
    pub server_ticks: u64,
    pub generator_ticks: u64,
}

/// Host-wide CPU pressure over the interval the run's CPU samples bracket:
/// growth of `/proc/pressure/cpu`'s `some` line `total=` counter, the
/// microseconds during which at least one runnable task waited for a CPU.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostCpuPressure {
    /// Monotonic nanoseconds between the start and the end sample — the
    /// same interval as `CpuUsage.sampled_ns`.
    pub sampled_ns: u64,
    /// Growth of the `some` line's `total=` counter, in microseconds.
    pub some_stall_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Env {
    pub kernel: String,
    pub cpu_model: String,
    pub host: String,
    pub git_sha: String,
    pub git_dirty: bool,
    pub tools: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeneratorErrors {
    pub connect: u64,
    pub protocol: u64,
    pub missed_slots: u64,
    pub stalled: u64,
    pub lateness_over_range: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConfigKey {
    pub server: ServerKind,
    pub workers: u32,
    pub qos: Option<u8>,
    pub kind: RunKind,
    pub offered_rate: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunRecord {
    pub schema: u32,
    pub profile: Profile,
    pub server: ServerKind,
    pub workers: u32,
    pub qos: Option<u8>,
    pub kind: RunKind,
    pub offered_rate: Option<u64>,
    pub repetition: u32,
    pub attempt: u32,
    pub connections: u32,
    pub payload_bytes: u32,
    pub qos1_window: u32,
    pub warmup_secs: u64,
    pub window_secs: u64,
    pub drain_grace_secs: u64,
    pub build: Build,
    pub origin_ns: u64,
    pub window: Window,
    pub timeline: Timeline,
    pub cycles_in_window: Option<u64>,
    pub counts: Counts,
    pub throughput_msgs_per_sec: Option<f64>,
    pub last10s_msgs_per_sec: Option<f64>,
    pub latency_ns: Option<LatencySummary>,
    pub lateness: Vec<(u64, u64)>,
    pub lateness_p99_ns: Option<u64>,
    pub last_write_start_ns: Option<u64>,
    pub connections_per_sec: Option<f64>,
    pub generator_errors: GeneratorErrors,
    pub syscalls: Syscalls,
    pub driver: Option<DriverFds>,
    pub cpu_sets: CpuSets,
    pub env: Env,
    pub cpu: Option<CpuUsage>,
    /// Host CPU pressure over the interval `cpu` covers (the window, or less
    /// for a run cancelled before its window ended); `None` when the pressure
    /// file was unreadable or unparsable at either sample, or the run returned
    /// before its window samples. Records written before this field existed
    /// read as `None`.
    #[serde(default)]
    pub host_cpu_pressure: Option<HostCpuPressure>,
    /// The run's connections split by the server worker that served them,
    /// busiest first, padded to `workers` with zero entries for workers that
    /// delivered no benchmark publish (all of them in a connections run). `None` for a
    /// mosquitto run, a run whose endpoint wrote no report, and records written
    /// before this field existed. Evidence only: no reconciliation row reads it.
    #[serde(default)]
    pub worker_loads: Option<Vec<WorkerLoad>>,
    pub started_at_unix: u64,
    pub harness_errors: Vec<String>,
    pub valid: bool,
    pub invalid_reasons: Vec<String>,
    /// `true` when SIGINT/SIGTERM had already been received by the time the
    /// run returned: the run may have been cut short, so it is no repetition
    /// of its attempt. Set by `matrix::run_configs`; records written before
    /// this field existed read as `false`.
    #[serde(default)]
    pub cancelled: bool,
}

fn server_lower(server: ServerKind) -> &'static str {
    match server {
        ServerKind::Uring => "uring",
        ServerKind::Tokio => "tokio",
        ServerKind::Mosquitto => "mosquitto",
    }
}

fn kind_lower(kind: RunKind) -> &'static str {
    match kind {
        RunKind::Throughput => "throughput",
        RunKind::Latency => "latency",
        RunKind::Connections => "connections",
        RunKind::Counted => "counted",
    }
}

impl RunRecord {
    pub fn key(&self) -> ConfigKey {
        ConfigKey {
            server: self.server,
            workers: self.workers,
            qos: self.qos,
            kind: self.kind,
            offered_rate: self.offered_rate,
        }
    }

    /// File naming convention used by `bench run` and `bench doc`:
    /// `<server>-w<W>[-q<Q>]-<kind>[-at<offered_rate>]-a<attempt>-r<rep>.json`,
    /// all names lowercased.
    pub fn file_name(&self) -> String {
        let q = self
            .qos
            .map_or_else(String::new, |n| format!("-q{n}"));
        let rate = self
            .offered_rate
            .map_or_else(String::new, |r| format!("-at{r}"));
        format!(
            "{}-w{}{}-{}{}-a{}-r{}.json",
            server_lower(self.server),
            self.workers,
            q,
            kind_lower(self.kind),
            rate,
            self.attempt,
            self.repetition,
        )
    }
}