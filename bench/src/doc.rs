//! `bench doc` (task 12 of the benchmark-harness feature): load the
//! records written by `bench run`, recompute and validate every one
//! against the fixed recorded matrix and its protocol invariants, and
//! emit the `BENCHMARKS.md` document. `render` refuses anything
//! unpublishable (AC-47); the resulting table renders the median
//! throughput, p99 latencies, syscalls/msg and driver proof per server
//! (AC-48); the document ends with the unticked sign-off checklist
//! (AC-49). Signatures match `features/benchmark-harness/PLAN.md`
//! `## Interface Contracts`.

// Rates, ns values and clock ticks are 64-bit and have to land in `f64`
// for ratios; the matching `f64 → u64` conversions are bounded by the
// spec (pct, ns). Accept the loss of precision; the bounds keep the
// truncation well-defined.
#![allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]

use std::collections::{BTreeSet, HashMap};
use std::fmt::Write as _;
use std::path::Path;

use crate::analysis::{config_passes, cpu_percent, metric};
use crate::matrix::{read_records, servers_for, RECORDED_QOS, RECORDED_SERVERS, RECORDED_WORKERS};
use crate::payload::PAYLOAD_BYTES;
use crate::run_record::{
    Build, ConfigKey, Profile, RunKind, RunRecord, ServerKind, SyscallMethod, SCHEMA_VERSION,
};
use crate::runner::{
    CONNECTIONS, LATENCY_FRACTIONS_PCT, ProtocolConstants, QOS1_WINDOW, SYSCALL_FRACTION_PCT,
};

/// Cell text for a configuration with no passing attempt: the table
/// never shows a figure taken from a failed run.
const FAILED: &str = "FAILED";

/// Server cell of a mosquitto row in `## Results`: §9.1 measures
/// mosquitto at a subscriber, not at the callback, and orders the
/// document to say so in those rows.
const MOSQUITTO_RESULTS_LABEL: &str = "mosquitto (endpoint = subscriber receipt)";

/// Reasons `bench doc` can refuse to render a document. Every variant
/// is the operator's only signal that an unpublishable record set exists;
/// tests assert on these variants one-for-one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocError {
    /// `dir` (or `records`) held no `*.json` records.
    NoRecords,
    /// A record declares a `schema` this version of `bench` does not know.
    UnsupportedSchema(u32),
    /// A `--smoke` record leaked into a recorded document.
    SmokeRecordInRecordedDoc(String),
    /// Recorded mode forbids a debug build.
    DebugBuild,
    /// Recorded mode forbids a dirty source tree.
    DirtyTree,
    /// Recorded mode forbids more than one distinct value in the named
    /// `env` field across records.
    MixedEnvironment(String),
    /// Recorded mode requires the shared `git_sha`, `kernel`, `host` or
    /// `cpu_model` to render a non-blank cell; holds the field name.
    BlankEnvironment(String),
    /// Recorded mode requires `env.tools` to list at least one probed
    /// tool: the document must identify the binaries behind its figures.
    NoToolVersions,
    /// Recorded mode requires every `env.tools` entry to carry a
    /// non-blank path; holds the zero-based position of the first entry
    /// whose path renders blank.
    BlankToolPath(usize),
    /// Recorded mode requires every `env.tools` entry to carry a version
    /// that renders as a non-blank cell; holds the tool path of the first
    /// entry whose version holds nothing but whitespace and control
    /// characters.
    BlankToolVersion(String),
    /// Recorded mode requires the shared `cpu_sets.server` and
    /// `cpu_sets.generator` to render a non-blank cell; holds the field
    /// name, `server` or `generator`, of the first blank one.
    BlankCpuSet(String),
    /// `analysis::recompute` flagged a record whose derived fields
    /// contradict its raw observations.
    InconsistentRecord(String),
    /// A recorded-mode protocol invariant was violated: timing
    /// constants, the cpu-set pair, the matched-rate, or the
    /// counted counter evidence.
    ProtocolViolation(String),
    /// A fixed-matrix configuration is missing or its highest attempt
    /// failed `config_passes(runs, 5, true)`.
    FailedConfigurations(Vec<String>),
}

/// Read every record from `dir`. Delegates to
/// `matrix::read_records` so the on-disk layout has a single reader
/// (task 11 already exposes it for `--resume`).
pub fn load_records(dir: &Path) -> Result<Vec<RunRecord>, String> {
    read_records(dir)
}

/// Render the `BENCHMARKS.md` document for `records`. `smoke = true`
/// skips the recorded-profile checks and produces a document with the
/// `SMOKE — not publishable` header (task 13's CI gate).
#[allow(clippy::too_many_lines)]
pub fn render(records: &[RunRecord], smoke: bool) -> Result<String, DocError> {
    if records.is_empty() {
        return Err(DocError::NoRecords);
    }

    // Row 1 — schema. The first record with a foreign schema decides,
    // because every later check assumes the schema `derive` /
    // `reconcile` / `cpu_percent` know how to read.
    for rec in records {
        if rec.schema != SCHEMA_VERSION {
            return Err(DocError::UnsupportedSchema(rec.schema));
        }
    }

    // Row 2 — recompute. A record whose stored summary no longer
    // follows from its own raw observations decides the run,
    // independent of any later field-by-field check.
    for rec in records {
        let mismatches = crate::analysis::recompute(rec);
        if !mismatches.is_empty() {
            return Err(DocError::InconsistentRecord(format!(
                "{}: {}",
                rec.file_name(),
                mismatches.join("; ")
            )));
        }
    }

    if !smoke {
        if let Some(name) = records.iter().find_map(|r| {
            if r.profile == Profile::Smoke {
                Some(r.file_name())
            } else {
                None
            }
        }) {
            return Err(DocError::SmokeRecordInRecordedDoc(name));
        }
        if records.iter().any(|r| r.build == Build::Debug) {
            return Err(DocError::DebugBuild);
        }
        if records.iter().any(|r| r.env.git_dirty) {
            return Err(DocError::DirtyTree);
        }
        let mut git_shas = BTreeSet::new();
        let mut kernels = BTreeSet::new();
        let mut hosts = BTreeSet::new();
        let mut cpu_models = BTreeSet::new();
        let mut tool_sets = BTreeSet::new();
        for rec in records {
            git_shas.insert(rec.env.git_sha.clone());
            kernels.insert(rec.env.kernel.clone());
            hosts.insert(rec.env.host.clone());
            cpu_models.insert(rec.env.cpu_model.clone());
            tool_sets.insert(rec.env.tools.clone());
        }
        if git_shas.len() > 1 {
            return Err(DocError::MixedEnvironment("git_sha".to_string()));
        }
        if kernels.len() > 1 {
            return Err(DocError::MixedEnvironment("kernel".to_string()));
        }
        // `## Environment` prints one host and one CPU model for every
        // figure in the document. A second value makes that disclosure
        // false for the records it does not name, so the environment
        // fields the document displays are checked like the commit and
        // the kernel.
        if hosts.len() > 1 {
            return Err(DocError::MixedEnvironment("host".to_string()));
        }
        if cpu_models.len() > 1 {
            return Err(DocError::MixedEnvironment("cpu_model".to_string()));
        }
        // `env.tools` is the record's only statement of which perf,
        // strace and mosquitto build produced its numbers, and a
        // `--resume` captures it again from whatever `--perf-bin` /
        // `--mosquitto-bin` the restart named. A second value therefore
        // means the medians combine measurements taken against
        // different binaries, which `## Environment` cannot disclose
        // from one record, so it is refused like the commit and the
        // kernel.
        if tool_sets.len() > 1 {
            return Err(DocError::MixedEnvironment("tools".to_string()));
        }
        // `## Environment` prints each of these fields as one cell for
        // every figure in the document, and every record shares one
        // value of each at this point. A value that `cell` renders blank
        // publishes a cell that identifies nothing, so the field is named
        // instead.
        let disclosed = [
            ("git_sha", &git_shas),
            ("kernel", &kernels),
            ("host", &hosts),
            ("cpu_model", &cpu_models),
        ];
        if let Some((field, _)) = disclosed
            .iter()
            .find(|(_, values)| values.iter().any(|v| cell(v).trim().is_empty()))
        {
            return Err(DocError::BlankEnvironment((*field).to_string()));
        }
        // A document must name the binaries behind its figures. Every
        // record shares one `env.tools` value at this point, so an empty
        // one means no tool version was identified for any figure.
        if tool_sets.iter().any(Vec::is_empty) {
            return Err(DocError::NoToolVersions);
        }

        // A path that `cell` renders blank publishes a tool row that
        // names no binary. Every record shares one `env.tools` value at
        // this point, so the position is the entry's index in each
        // record. Checked over every entry before any version, so an
        // entry with both blank names its position rather than an
        // empty path.
        if let Some(index) = tool_sets
            .iter()
            .flatten()
            .position(|(path, _)| cell(path).trim().is_empty())
        {
            return Err(DocError::BlankToolPath(index));
        }

        // Every entry becomes one `## Environment` row, so a blank
        // version leaves that tool unidentified while the others are
        // named.
        if let Some((path, _)) = tool_sets
            .iter()
            .flatten()
            .find(|(_, version)| cell(version).trim().is_empty())
        {
            return Err(DocError::BlankToolVersion(path.clone()));
        }

        // Protocol invariants that name a single field across every
        // record (timing constants, single cpu-set pair). Reject on the
        // first violation so the operator's message names one cause.
        let constants = ProtocolConstants::for_profile(Profile::Recorded);
        for rec in records {
            if rec.warmup_secs != constants.warmup_secs {
                return Err(DocError::ProtocolViolation(format!(
                    "{}: warmup_secs {} != {}",
                    rec.file_name(),
                    rec.warmup_secs,
                    constants.warmup_secs
                )));
            }
            if rec.window_secs != constants.window_secs {
                return Err(DocError::ProtocolViolation(format!(
                    "{}: window_secs {} != {}",
                    rec.file_name(),
                    rec.window_secs,
                    constants.window_secs
                )));
            }
            if rec.drain_grace_secs != constants.drain_grace_secs {
                return Err(DocError::ProtocolViolation(format!(
                    "{}: drain_grace_secs {} != {}",
                    rec.file_name(),
                    rec.drain_grace_secs,
                    constants.drain_grace_secs
                )));
            }
            if rec.connections != CONNECTIONS {
                return Err(DocError::ProtocolViolation(format!(
                    "{}: connections {} != {}",
                    rec.file_name(),
                    rec.connections,
                    CONNECTIONS
                )));
            }
            if rec.payload_bytes != u32::try_from(PAYLOAD_BYTES).expect("PAYLOAD_BYTES fits in u32") {
                return Err(DocError::ProtocolViolation(format!(
                    "{}: payload_bytes {} != {}",
                    rec.file_name(),
                    rec.payload_bytes,
                    PAYLOAD_BYTES
                )));
            }
            if rec.qos1_window != QOS1_WINDOW {
                return Err(DocError::ProtocolViolation(format!(
                    "{}: qos1_window {} != {}",
                    rec.file_name(),
                    rec.qos1_window,
                    QOS1_WINDOW
                )));
            }
        }
        let mut cpu_set_pairs = BTreeSet::new();
        for rec in records {
            cpu_set_pairs.insert((rec.cpu_sets.server.clone(), rec.cpu_sets.generator.clone()));
        }
        if cpu_set_pairs.len() > 1 {
            return Err(DocError::ProtocolViolation(format!(
                "multiple cpu_sets pairs: {cpu_set_pairs:?}"
            )));
        }
        // `## Environment` prints the shared pair as one cell for every
        // figure, and the CPU % columns divide by the CPUs it names. A
        // set that `cell` renders blank identifies no CPU and leaves every
        // CPU figure `n/a`, so the field is named instead. Checked after
        // the pair check, so a set that disagrees still reports as mixed.
        if let Some((server, generator)) = cpu_set_pairs.iter().next() {
            if cell(server).trim().is_empty() {
                return Err(DocError::BlankCpuSet("server".to_string()));
            }
            if cell(generator).trim().is_empty() {
                return Err(DocError::BlankCpuSet("generator".to_string()));
            }
        }
    }

    // Completeness — every expected configuration must exist and its
    // highest attempt must pass `config_passes(runs, 5, true)`. Done
    // first so the matched-rate calculation always has a throughput
    // median to read. Smoke mode does not require completeness; the
    // table marks missing or failed configurations `FAILED` in their
    // cells instead.
    let expected = expected_configs();
    let mut by_key: HashMap<ConfigKey, Vec<RunRecord>> = HashMap::new();
    for rec in records {
        by_key.entry(rec.key()).or_default().push(rec.clone());
    }
    // A recorded document discloses the fixed matrix and nothing else.
    // The loops below visit the expected keys only, so a record of any
    // other configuration — a two-worker row, a third rate fraction —
    // would pass every per-record check and stay out of the document
    // while the run succeeds. Refuse it instead of publishing a
    // measurement the document does not name.
    if !smoke {
        for rec in records {
            let key = rec.key();
            if !expected.contains(&key) {
                return Err(DocError::ProtocolViolation(format!(
                    "{}: configuration {} is outside the recorded matrix",
                    rec.file_name(),
                    config_label(&key)
                )));
            }
        }
    }
    let constants = if smoke {
        ProtocolConstants::for_profile(Profile::Smoke)
    } else {
        ProtocolConstants::for_profile(Profile::Recorded)
    };
    let repetitions = constants.repetitions;
    let enforce_spread = constants.enforce_spread;
    let mut failed_names: Vec<String> = Vec::new();
    let mut passing_by_key: HashMap<ConfigKey, Vec<RunRecord>> = HashMap::new();
    for key in &expected {
        let Some(runs) = by_key.get(key) else {
            if !smoke {
                failed_names.push(config_label(key));
            }
            continue;
        };
        let mut highest_attempt = runs.iter().map(|r| r.attempt).max().unwrap_or(0);
        let mut attempt_runs: Vec<RunRecord> = runs
            .iter()
            .filter(|r| r.attempt == highest_attempt)
            .cloned()
            .collect();
        if attempt_runs.is_empty() {
            highest_attempt = 0;
            attempt_runs.clone_from(runs);
        }
        if config_passes(&attempt_runs, repetitions, enforce_spread) {
            passing_by_key.insert(*key, attempt_runs);
        } else if !smoke {
            failed_names.push(config_label(key));
        }
    }
    if !smoke && !failed_names.is_empty() {
        return Err(DocError::FailedConfigurations(failed_names));
    }

    // Recorded-mode protocol invariants attached to one (or all) of
    // the configurations: counter evidence on the counted rows only,
    // one syscall method, matched-rate alignment. The repetition set is
    // `config_passes`' job, already enforced above.
    if !smoke {
        for key in &expected {
            if let Some(runs) = by_key.get(key) {
                // Counter evidence is a property of every counted
                // record, not only of the attempt the table publishes:
                // a recorded session counts syscalls in all of them, so
                // an attempt without a counter is a record made under
                // another harness configuration.
                if key.kind == RunKind::Counted {
                    if let Some(bad) = runs.iter().find(|r| {
                        !matches!(r.syscalls.method, SyscallMethod::Perf | SyscallMethod::Strace)
                    }) {
                        return Err(DocError::ProtocolViolation(format!(
                            "{}: counted record without counter evidence ({})",
                            config_label(key),
                            bad.file_name()
                        )));
                    }
                } else if let Some((bad, reason)) = runs
                    .iter()
                    .find_map(|r| counter_on_uncounted_row(r).map(|reason| (r, reason)))
                {
                    return Err(DocError::ProtocolViolation(format!(
                        "{}: {reason} on a row the matrix does not count ({})",
                        config_label(key),
                        bad.file_name()
                    )));
                }
            }
        }
        // One syscall method per document. The table carries a single
        // `syscalls/msg @80 %` column and a single method label, so two
        // methods across the published attempts make those figures a
        // mixed-method comparison and leave strace's bias disclosure
        // attached to figures `perf` produced.
        let methods = published_syscall_methods(&passing_by_key);
        if methods.len() > 1 {
            return Err(DocError::ProtocolViolation(format!(
                "mixed syscall counter methods: {}",
                methods.iter().copied().collect::<Vec<_>>().join(", ")
            )));
        }
        // Matched-rate alignment: per (w, q) and pct, the latency
        // `offered_rate` must equal
        // `⌊pct / 100 × min(throughput medians of the passing attempts of (w, q))⌋`.
        for w in RECORDED_WORKERS {
            let servers = servers_for(w, &RECORDED_SERVERS);
            for q in RECORDED_QOS {
                let throughput_medians: Vec<f64> = servers
                    .iter()
                    .filter_map(|s| {
                        let key = ConfigKey {
                            server: *s,
                            workers: w,
                            qos: Some(q),
                            kind: RunKind::Throughput,
                            rate_fraction_pct: None,
                        };
                        let runs = passing_by_key.get(&key)?;
                        let mut metrics: Vec<f64> = runs.iter().filter_map(metric).collect();
                        if metrics.is_empty() {
                            None
                        } else {
                            metrics.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
                            Some(crate::analysis::median(&metrics).unwrap_or(0.0))
                        }
                    })
                    .collect();
                let Some(min) = throughput_medians
                    .iter()
                    .copied()
                    .reduce(f64::min)
                else {
                    continue;
                };
                for &s in &servers {
                    for (kind, pct) in LATENCY_FRACTIONS_PCT
                        .iter()
                        .map(|&pct| (RunKind::Latency, pct))
                        .chain(std::iter::once((RunKind::Counted, SYSCALL_FRACTION_PCT)))
                    {
                        let key = ConfigKey {
                            server: s,
                            workers: w,
                            qos: Some(q),
                            kind,
                            rate_fraction_pct: Some(pct),
                        };
                        let Some(runs) = by_key.get(&key) else { continue };
                        let expected_rate = (f64::from(pct) / 100.0 * min).floor() as u64;
                        // Every attempt of a latency configuration is
                        // paced at the same matched rate: `run_matrix`
                        // derives the rate from the passing throughput
                        // records before the first latency run and keeps
                        // it for the re-runs. A record at another rate
                        // measured a different experiment, so it decides
                        // the document even when a later attempt passes.
                        for r in runs {
                            if r.offered_rate != Some(expected_rate) {
                                return Err(DocError::ProtocolViolation(format!(
                                    "{}: offered_rate {:?} != {expected_rate} (matched rate) ({})",
                                    config_label(&key),
                                    r.offered_rate,
                                    r.file_name()
                                )));
                            }
                        }
                    }
                }
            }
        }
    }

    // Build the document.
    let any_counted = records.iter().any(|r| r.kind == RunKind::Counted);
    let mut out = String::new();
    writeln!(out, "# Benchmarks").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "> A generated file produced by `bench doc`. Do not edit by hand."
    )
    .unwrap();
    writeln!(out).unwrap();
    if smoke {
        writeln!(out, "> **SMOKE — not publishable.**").unwrap();
        writeln!(out).unwrap();
    }

    // ## Environment
    let sample = &records[0];
    let server_cpus = &sample.cpu_sets.server;
    let gen_cpus = &sample.cpu_sets.generator;
    let method_label = syscalls_method_label(&passing_by_key);
    writeln!(out, "## Environment").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "| Field | Value |").unwrap();
    writeln!(out, "|---|---|").unwrap();
    writeln!(out, "| Host | {} |", cell(&sample.env.host)).unwrap();
    writeln!(out, "| CPU model | {} |", cell(&sample.env.cpu_model)).unwrap();
    writeln!(
        out,
        "| Kernel | {} (WSL2 under the Hyper-V scheduler; hybrid P/E cores are not exposed by WSL) |",
        cell(&sample.env.kernel)
    )
    .unwrap();
    writeln!(
        out,
        "| CPU sets | server `{}`, generator `{}` |",
        cell(server_cpus),
        cell(gen_cpus)
    )
    .unwrap();
    writeln!(out, "| Commit | {} |", cell(&sample.env.git_sha)).unwrap();
    writeln!(
        out,
        "| Build | {} |",
        if sample.build == Build::Release {
            "release"
        } else {
            "debug"
        }
    )
    .unwrap();
    writeln!(out, "| Syscall method | {method_label} |").unwrap();
    // One row per probed tool: recorded mode holds a single `env.tools`
    // value, so these versions name the binaries behind every figure in
    // the document.
    for (path, version) in &sample.env.tools {
        writeln!(
            out,
            "| Tool `{}` | {} |",
            cell(path),
            cell(version)
        )
        .unwrap();
    }
    writeln!(out).unwrap();

    writeln!(out, "## Method").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "Constants: warm-up {} s, window {} s, {} repetitions, {} connections, \
         {}-byte payloads, qos1_window {}, MQTT 3.1.1. Closed-loop throughput \
         measures msgs/s at the callback; open-loop latency paces sends at \
         `⌊0.5 / 0.8 × slowest competitor⌋` msgs/s of the same `(w, q)` row, \
         the matched latency rate. Mosquitto endpoint = subscriber receipt.",
        constants.warmup_secs,
        constants.window_secs,
        repetitions,
        CONNECTIONS,
        PAYLOAD_BYTES,
        QOS1_WINDOW
    )
    .unwrap();
    writeln!(out).unwrap();
    if any_counted {
        writeln!(
            out,
            "Both latency columns come from uncounted runs; `syscalls/msg` comes \
             from {repetitions} separate counted repetitions at the same matched \
             {SYSCALL_FRACTION_PCT} % rate, which qualify on `syscalls/msg` spread. \
             The counted runs' p99 stays in the raw JSON and is not published.",
        )
        .unwrap();
    } else {
        writeln!(
            out,
            "Both latency columns come from uncounted runs; `syscalls/msg` is `n/a` \
             because this record set holds no counted run.",
        )
        .unwrap();
    }
    writeln!(out).unwrap();

    // Per (w, q) results
    for w in RECORDED_WORKERS {
        let servers = servers_for(w, &RECORDED_SERVERS);
        for q in RECORDED_QOS {
            writeln!(
                out,
                "## Results — {w} worker(s), QoS {q}"
            )
            .unwrap();
            writeln!(out).unwrap();
            writeln!(
                out,
                "| Server | msgs/s | spread | server CPU % | generator CPU % | \
                 p99 @50 % (µs) | p99 @80 % (µs) | syscalls/msg @80 % | driver proof |"
            )
            .unwrap();
            writeln!(out, "|---|---|---|---|---|---|---|---|---|").unwrap();
            for &s in &servers {
                let row = results_row(&passing_by_key, s, w, q, any_counted);
                writeln!(out, "{row}").unwrap();
            }
            writeln!(out).unwrap();
        }

        // Connections table
        writeln!(
            out,
            "## Connections per second — {w} worker(s)"
        )
        .unwrap();
        writeln!(out).unwrap();
        writeln!(out, "| Server | conn/s | spread |").unwrap();
        writeln!(out, "|---|---|---|").unwrap();
        for &s in &servers {
            let row = connections_row(&passing_by_key, s, w);
            writeln!(out, "{row}").unwrap();
        }
        writeln!(out).unwrap();
    }

    writeln!(out, "## Sign-off").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "- [ ] Driver proof: every uring run shows io_uring fds = workers and eventpoll fds = 0"
    )
    .unwrap();
    writeln!(
        out,
        "- [ ] Spread: every configuration within 10 % over 5 interleaved repetitions"
    )
    .unwrap();
    writeln!(
        out,
        "- [ ] Reconciliation: every run reconciled (unresolved 0, duplicates 0, dropped 0 in throughput runs)"
    )
    .unwrap();
    writeln!(
        out,
        "- [ ] Environment: host, kernel, CPU sets, AC power and idle state match this disclosure"
    )
    .unwrap();
    writeln!(
        out,
        "- [ ] Saturation: in every throughput row the server CPU set, not the generator, is the saturated side"
    )
    .unwrap();

    Ok(out)
}

/// One throughput / latency row in `## Results — w, q`. Every cell is
/// rendered from the passing highest attempt of its own configuration,
/// so a retried configuration never mixes a failed attempt into a
/// published figure. A cell whose configuration has no passing attempt
/// prints `FAILED`.
fn results_row(
    passing: &HashMap<ConfigKey, Vec<RunRecord>>,
    server: ServerKind,
    workers: u32,
    qos: u8,
    any_counted: bool,
) -> String {
    let thr_key = ConfigKey {
        server,
        workers,
        qos: Some(qos),
        kind: RunKind::Throughput,
        rate_fraction_pct: None,
    };
    let lat50_key = ConfigKey {
        server,
        workers,
        qos: Some(qos),
        kind: RunKind::Latency,
        rate_fraction_pct: Some(50u32),
    };
    let lat80_key = ConfigKey {
        server,
        workers,
        qos: Some(qos),
        kind: RunKind::Latency,
        rate_fraction_pct: Some(80u32),
    };
    let counted_key = ConfigKey {
        server,
        workers,
        qos: Some(qos),
        kind: RunKind::Counted,
        rate_fraction_pct: Some(SYSCALL_FRACTION_PCT),
    };

    // msgs/s, spread, the CPU shares and the driver proof all describe
    // the throughput configuration, so they stand or fall together.
    let (msgs, spread, server_cpu, gen_cpu, driver) = match passing.get(&thr_key) {
        Some(runs) => {
            let metrics: Vec<f64> = runs.iter().filter_map(metric).collect();
            let m = crate::analysis::median(&metrics).unwrap_or(0.0);
            let s = crate::analysis::spread(&metrics).unwrap_or(0.0) * 100.0;
            (
                format!("{m:.0}"),
                format!("{s:.1} %"),
                median_cpu_percent(runs, true),
                median_cpu_percent(runs, false),
                driver_label(runs, server, workers),
            )
        }
        None => (
            FAILED.to_string(),
            FAILED.to_string(),
            FAILED.to_string(),
            FAILED.to_string(),
            FAILED.to_string(),
        ),
    };

    format!(
        "| {server} | {msgs} | {spread} | {server_cpu} | {gen_cpu} | {p50} | {p80} | {sys} | {driver} |",
        server = if server == ServerKind::Mosquitto { MOSQUITTO_RESULTS_LABEL } else { server_label(server) },
        p50 = latency_p99_us(passing, &lat50_key),
        p80 = latency_p99_us(passing, &lat80_key),
        sys = syscalls_per_msg(passing, &counted_key, any_counted),
    )
}

fn connections_row(
    passing: &HashMap<ConfigKey, Vec<RunRecord>>,
    server: ServerKind,
    workers: u32,
) -> String {
    let key = ConfigKey {
        server,
        workers,
        qos: None,
        kind: RunKind::Connections,
        rate_fraction_pct: None,
    };
    let Some(runs) = passing.get(&key) else {
        return format!("| {} | {FAILED} | {FAILED} |", server_label(server));
    };
    let metrics: Vec<f64> = runs.iter().filter_map(metric).collect();
    let m = crate::analysis::median(&metrics).unwrap_or(0.0);
    let s = crate::analysis::spread(&metrics).unwrap_or(0.0) * 100.0;
    format!(
        "| {server} | {m:.0} | {s:.1} % |",
        server = server_label(server)
    )
}

fn median_cpu_percent(runs: &[RunRecord], server: bool) -> String {
    let mut values: Vec<f64> = Vec::new();
    for r in runs {
        let Some(cpu) = r.cpu else { continue };
        let cpus = if server {
            crate::analysis::cpu_count(&r.cpu_sets.server)
        } else {
            crate::analysis::cpu_count(&r.cpu_sets.generator)
        };
        let Some(cpus) = cpus else { continue };
        let ticks = if server {
            cpu.server_ticks
        } else {
            cpu.generator_ticks
        };
        if let Some(pct) = cpu_percent(ticks, cpu.clk_tck, cpu.sampled_ns, cpus) {
            values.push(pct);
        }
    }
    match crate::analysis::median(&values) {
        Some(m) => format!("{m:.1}"),
        None => "n/a".to_string(),
    }
}

/// Median p99 of the passing highest attempt of `key`, in µs. `FAILED`
/// when that configuration has no passing attempt: a p99 read off a
/// failed attempt is not a measurement of the configuration.
fn latency_p99_us(passing: &HashMap<ConfigKey, Vec<RunRecord>>, key: &ConfigKey) -> String {
    let Some(runs) = passing.get(key) else {
        return FAILED.to_string();
    };
    let mut values: Vec<u64> = runs
        .iter()
        .filter_map(|r| r.latency_ns.as_ref().map(|l| l.p99))
        .collect();
    if values.is_empty() {
        return "n/a".to_string();
    }
    values.sort_unstable();
    let median_ns = if values.len() % 2 == 1 {
        values[values.len() / 2]
    } else {
        let mid = values.len() / 2;
        // Take the midpoint via `u128` to avoid `u64` overflow on the
        // sums the (a + b) shortcut silently overflows.
        let sum = u128::from(values[mid - 1]) + u128::from(values[mid]);
        u64::try_from(sum / 2).expect("latency midpoint fits in u64")
    };
    format!("{:.1}", median_ns as f64 / 1000.0)
}

/// Median syscalls/message of the passing highest attempt of `key`.
/// `n/a` first when no record in `records` was a counted run (a smoke
/// set, or a recorded set the completeness check has already refused);
/// `FAILED` when the counted configuration has no passing attempt;
/// `n/a` when that attempt passed with no per-message figure.
fn syscalls_per_msg(
    passing: &HashMap<ConfigKey, Vec<RunRecord>>,
    key: &ConfigKey,
    any_counted: bool,
) -> String {
    if !any_counted {
        return "n/a".to_string();
    }
    let Some(runs) = passing.get(key) else {
        return FAILED.to_string();
    };
    let values: Vec<f64> = runs.iter().filter_map(|r| r.syscalls.per_msg).collect();
    if values.is_empty() {
        return "n/a".to_string();
    }
    let m = crate::analysis::median(&values).unwrap_or(0.0);
    format!("{m:.2}")
}

fn driver_label(runs: &[RunRecord], server: ServerKind, workers: u32) -> String {
    // §9.1 defines a driver proof for the monoio server only.
    if server != ServerKind::Uring {
        return "n/a".to_string();
    }
    if runs.is_empty() {
        return "n/a".to_string();
    }
    let all_match = runs.iter().all(|r| match (server, r.driver) {
        (ServerKind::Uring, Some(d)) => d.io_uring_fds == workers && d.eventpoll_fds == 0,
        _ => false,
    });
    if all_match {
        "ok".to_string()
    } else {
        "failed".to_string()
    }
}

/// `text` as the contents of one Markdown table cell.
///
/// Every string in `## Environment` comes from a record this process
/// read from disk, and `recompute` inspects numbers rather than text,
/// so a crafted record can hold any bytes in its `env` fields. Two
/// characters decide the document's structure rather than one cell's
/// text: a line break ends the table, so what follows it is read as new
/// headings and claims of the document itself, and `|` ends the cell.
/// Both are neutralized here — a line break becomes a space and `|` is
/// escaped, with the backslash escaped first so an escape in the value
/// cannot consume the one added here. Nothing else about the value is
/// changed: the published document shows what the record stored.
fn cell(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '|' => out.push_str("\\|"),
            c if c.is_control() => out.push(' '),
            c => out.push(c),
        }
    }
    out
}

fn server_label(server: ServerKind) -> &'static str {
    match server {
        ServerKind::Uring => "uring",
        ServerKind::Tokio => "tokio",
        ServerKind::Mosquitto => "mosquitto",
    }
}

/// Why `record` must not be published on a row the matrix does not
/// count, or `None` when it carries no counter.
///
/// `runner::should_count` attaches a counter to the counted run only,
/// so every run that reaches its window end on any other row stores
/// `SyscallMethod::None` with an empty block. `analysis::derive` reads
/// the count back out of `syscalls.raw` for any kind, so a record of
/// another row whose raw output, count and per-message figure agree
/// passes `recompute`: its server ran under a syscall tracer while the
/// rest of the matrix did not, and the document would publish its p99
/// beside figures measured without that instrument.
///
/// Only a counted run that failed before its window keeps
/// `host.syscalls` as the method with every field empty
/// (`runner::finalize_record`), which is not evidence of a counter.
/// Such a record is invalid and only ever published as a failed earlier
/// attempt, so the method is checked on valid records and the empty
/// block is accepted; every other run records `SyscallMethod::None`.
fn counter_on_uncounted_row(record: &RunRecord) -> Option<&'static str> {
    let s = &record.syscalls;
    if s.count.is_some()
        || s.per_msg.is_some()
        || s.raw.is_some()
        || s.enabled_ns.is_some()
        || s.disabled_ns.is_some()
        || s.exit.is_some()
        || s.diagnostics.is_some()
    {
        return Some("counter evidence");
    }
    if record.valid && s.method != SyscallMethod::None {
        return Some("counter method");
    }
    None
}

/// Methods behind the published `syscalls/msg @80 %` figures: the
/// counter methods of the passing counted attempts, which are the only
/// records the table reads those figures from.
fn published_syscall_methods(
    passing: &HashMap<ConfigKey, Vec<RunRecord>>,
) -> BTreeSet<&'static str> {
    let mut methods: BTreeSet<&'static str> = BTreeSet::new();
    for (key, runs) in passing {
        if key.kind != RunKind::Counted {
            continue;
        }
        for r in runs {
            methods.insert(match r.syscalls.method {
                SyscallMethod::Perf => "perf",
                SyscallMethod::Strace => "strace",
                SyscallMethod::None => "none",
            });
        }
    }
    methods
}

/// `| Syscall method |` cell of `## Environment`. `n/a` when no passing
/// counted attempt carries a figure. The strace bias disclosure follows
/// the label whenever strace produced any published figure; recorded mode
/// has already refused a mixed-method matrix, so a smoke document is the
/// only place the label can name more than one method.
fn syscalls_method_label(passing: &HashMap<ConfigKey, Vec<RunRecord>>) -> String {
    let methods = published_syscall_methods(passing);
    if methods.is_empty() {
        return "n/a".to_string();
    }
    let label = methods.iter().copied().collect::<Vec<_>>().join(", ");
    if methods.contains("strace") {
        format!(
            "{label} (ptrace slows each syscall, which raises completions per `io_uring_enter` and flatters io_uring)"
        )
    } else {
        label
    }
}

fn expected_configs() -> Vec<ConfigKey> {
    let mut out = Vec::new();
    for w in RECORDED_WORKERS {
        let servers = servers_for(w, &RECORDED_SERVERS);
        for q in RECORDED_QOS {
            for &s in &servers {
                out.push(ConfigKey {
                    server: s,
                    workers: w,
                    qos: Some(q),
                    kind: RunKind::Throughput,
                    rate_fraction_pct: None,
                });
                out.push(ConfigKey {
                    server: s,
                    workers: w,
                    qos: Some(q),
                    kind: RunKind::Latency,
                    rate_fraction_pct: Some(50),
                });
                out.push(ConfigKey {
                    server: s,
                    workers: w,
                    qos: Some(q),
                    kind: RunKind::Latency,
                    rate_fraction_pct: Some(80),
                });
                out.push(ConfigKey {
                    server: s,
                    workers: w,
                    qos: Some(q),
                    kind: RunKind::Counted,
                    rate_fraction_pct: Some(SYSCALL_FRACTION_PCT),
                });
            }
        }
        for &s in &servers {
            out.push(ConfigKey {
                server: s,
                workers: w,
                qos: None,
                kind: RunKind::Connections,
                rate_fraction_pct: None,
            });
        }
    }
    out
}

/// Kebab-case label for a configuration: `uring-w1-q1-throughput`,
/// `tokio-w4-connections`, `mosquitto-w1-q0-latency-50`. Used in
/// `FailedConfigurations` and `ProtocolViolation` so the operator's
/// message names a single, greppable configuration.
fn config_label(key: &ConfigKey) -> String {
    let kind = match key.kind {
        RunKind::Throughput => "throughput".to_string(),
        RunKind::Latency => match key.rate_fraction_pct {
            Some(pct) => format!("latency-{pct}"),
            None => "latency".to_string(),
        },
        RunKind::Connections => "connections".to_string(),
        RunKind::Counted => "counted".to_string(),
    };
    let qos = key
        .qos
        .map(|q| format!("-q{q}"))
        .unwrap_or_default();
    format!("{server}-w{workers}{qos}-{kind}", server = server_label(key.server), workers = key.workers)
}