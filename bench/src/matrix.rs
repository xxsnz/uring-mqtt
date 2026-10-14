//! Matrix orchestration (task 11 of the benchmark-harness feature):
//! `servers_for` filters mosquitto out of the 4-worker rows (AC-44),
//! `check_request` validates the CLI inputs before any process is spawned
//! (AC-42, AC-43), `run_configs` runs the distinct configurations
//! through `interleave`d repetitions with attempt-based re-runs whose
//! failed attempts stay on disk (AC-46), `read_records`/`check_resume`/
//! `resume_plan` plan a `--resume` restart that skips passed configs
//! and continues the others from the next attempt (AC-62, AC-63), and
//! `run_matrix` drives the §9.1 matrix — throughput first, then
//! matched-rate latency, then connections — with SIGINT/SIGTERM
//! cancellation (AC-52). Signatures and constants match
//! `features/benchmark-harness/PLAN.md` `## Interface Contracts`.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::analysis::{
    attempt_complete, config_failure, config_passes, interleave, median, matched_rates, recompute,
};
use crate::run_record::{
    ConfigKey, CpuSets, Env, Profile, RunKind, RunRecord, ServerKind, SyscallMethod, SCHEMA_VERSION,
};
use crate::runner::{
    HostSetup, stem_for, LATENCY_FRACTIONS_PCT, ProtocolConstants, RunSpec, SCRATCH_REUSED_PREFIX,
    SYSCALL_FRACTION_PCT,
};

/// The full server set §9.1 records. A `bench run --profile recorded`
/// refuses any other combination (AC-42).
pub const RECORDED_SERVERS: [ServerKind; 3] = [
    ServerKind::Uring,
    ServerKind::Tokio,
    ServerKind::Mosquitto,
];

/// Worker counts §9.1 records.
pub const RECORDED_WORKERS: [u32; 2] = [1, 4];

/// QoS levels §9.1 records.
pub const RECORDED_QOS: [u8; 2] = [1, 0];

/// What the operator asked the matrix to do. `resume` switches the
/// re-run logic to the `read_records`/`resume_plan` path.
#[derive(Debug, Clone)]
pub struct MatrixSpec {
    pub profile: Profile,
    pub servers: Vec<ServerKind>,
    pub workers: Vec<u32>,
    pub qos: Vec<u8>,
    pub out_dir: PathBuf,
    pub resume: bool,
}

/// One entry in [`resume_plan`]. `Passed` means the highest attempt
/// already passed and the config does not run again; `NextAttempt`
/// carries the next attempt number and the completed attempts still
/// available.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeState {
    Passed,
    /// The highest attempt did not pass. `attempt` is the number the
    /// next run uses; `remaining` is how many more completed attempts
    /// the configuration may run (`max_config_attempts` minus its
    /// completed attempts, 0 when `attempt` cannot advance).
    NextAttempt { attempt: u32, remaining: u32 },
}

/// What [`run_matrix`] (and [`run_configs`]) produced. `passed` are the
/// configurations whose every attempt passed `config_passes`; `failed`
/// are the ones that ran through every `max_config_attempts` and still
/// did not.
#[derive(Debug, Clone, Default)]
pub struct MatrixOutcome {
    pub passed: Vec<ConfigKey>,
    pub failed: Vec<ConfigKey>,
}

/// Mosquitto is single-process, single-port, so it cannot run on more
/// than one worker. Every other server runs on any worker count the
/// `MatrixSpec` lists.
pub fn servers_for(workers: u32, servers: &[ServerKind]) -> Vec<ServerKind> {
    if workers == 1 {
        servers.to_vec()
    } else {
        servers
            .iter()
            .copied()
            .filter(|s| *s != ServerKind::Mosquitto)
            .collect()
    }
}

/// Refuse a `bench run` request before any process is spawned. Every
/// profile requires distinct matrix values inside the domains the
/// harness runs, at least one runnable configuration, an executable for
/// the counter it selected and no path for a counter it did not, and an
/// out_dir consistent with `--resume`; recorded runs also require a
/// syscall counter, a mosquitto binary, and the full §9.1 matrix;
/// nothing the rejection fires is a tool-version probe, so the
/// marker-tool test can prove the refusal happened before any
/// subprocess started (AC-15).
pub fn check_request(
    spec: &MatrixSpec,
    syscalls: SyscallMethod,
    perf_bin: Option<&Path>,
    strace_bin: Option<&Path>,
    mosquitto_bin: Option<&Path>,
) -> Result<(), String> {
    check_distinct("servers", &spec.servers)?;
    check_distinct("workers", &spec.workers)?;
    check_distinct("qos", &spec.qos)?;
    check_domains(spec)?;
    check_runnable(spec)?;
    check_counter_bin(syscalls, perf_bin, strace_bin)?;
    if spec.profile == Profile::Recorded {
        if syscalls == SyscallMethod::None {
            return Err("recorded profile requires --syscalls perf|strace".to_string());
        }
        if mosquitto_bin.is_none() {
            return Err("recorded profile requires --mosquitto-bin".to_string());
        }
        if spec.servers.as_slice() != RECORDED_SERVERS.as_slice() {
            return Err("recorded profile runs the full matrix".to_string());
        }
        if spec.workers.as_slice() != RECORDED_WORKERS.as_slice() {
            return Err("recorded profile runs the full matrix".to_string());
        }
        if spec.qos.as_slice() != RECORDED_QOS.as_slice() {
            return Err("recorded profile runs the full matrix".to_string());
        }
    }
    let has_json = out_dir_has_json(&spec.out_dir)?;
    if !spec.resume {
        if has_json {
            return Err("output directory is not empty".to_string());
        }
    } else if !has_json {
        return Err("nothing to resume".to_string());
    }
    Ok(())
}

/// Refuse a matrix list that repeats a value. `run_matrix` walks
/// `servers`, `workers` and `qos` exactly as given, so a repeated value
/// dispatches the same `ConfigKey` twice at the same attempt and
/// repetition: the second dispatch finds its scratch directory already
/// there, and the invalid record it returns would replace the first
/// traversal's valid JSON under the same `file_name()`.
fn check_distinct<T: PartialEq + std::fmt::Debug>(
    label: &str,
    values: &[T],
) -> Result<(), String> {
    for (i, value) in values.iter().enumerate() {
        if values[..i].contains(value) {
            return Err(format!("--{label} lists {value:?} twice"));
        }
    }
    Ok(())
}

/// Refuse a matrix value outside the domain the harness measures. The
/// endpoint and the generator validate their own single configuration
/// only once a run reaches them (`bench/src/gen.rs`,
/// `qos must be 0 or 1`), and `bench server` does not bound its worker
/// count at all: without this check `--qos 2` spawns an endpoint and
/// fails in the generator, and `--workers 0` reaches server startup.
/// Both leave failed-run records and exit 2 for a request that is
/// malformed before any process exists.
fn check_domains(spec: &MatrixSpec) -> Result<(), String> {
    for &qos in &spec.qos {
        if qos > 1 {
            return Err(format!("--qos lists {qos}; qos must be 0 or 1"));
        }
    }
    if spec.workers.contains(&0) {
        return Err("--workers lists 0; a server runs at least one worker".to_string());
    }
    Ok(())
}

/// Refuse a matrix that no server can run. `servers_for` filters
/// mosquitto out of every multi-worker row (AC-44), so a request like
/// `--servers mosquitto --workers 4` leaves every stage of `run_matrix`
/// without a single configuration: no record is written, `outcome.failed`
/// stays empty, and the command exits 0 reporting a run that measured
/// nothing. An empty worker list has the same effect.
fn check_runnable(spec: &MatrixSpec) -> Result<(), String> {
    if spec
        .workers
        .iter()
        .all(|&w| servers_for(w, &spec.servers).is_empty())
    {
        return Err("no server runs on any requested worker count".to_string());
    }
    Ok(())
}

/// Refuse a selected syscall counter that has no executable, and refuse
/// an executable for a counter the selection did not pick (AC-15).
/// `run_one` reports a missing counter path only once a counted run
/// reaches the counter (`bench/src/runner.rs`, `no perf_bin`/`no
/// strace_bin`), so without the missing-check half the matrix runs
/// every earlier configuration and then invalidates every counted one
/// through all of its attempts. Without the unused-check half a
/// `capture_env` probe runs `tool --version` for a flag the run never
/// reads, so a `--syscalls strace` request that names `--perf-bin`
/// spends a subprocess on its first refusal only.
///
/// The unused path is reported first: AC-15 names the `does not use`
/// refusal for every request that carries an unused counter path, and
/// `--syscalls perf --strace-bin <path>` with no `--perf-bin` carries
/// one. Naming the missing path first would answer that request with
/// `--syscalls perf requires --perf-bin`, which sends the operator to
/// add a second path rather than to drop the one the selection does not
/// read.
fn check_counter_bin(
    syscalls: SyscallMethod,
    perf_bin: Option<&Path>,
    strace_bin: Option<&Path>,
) -> Result<(), String> {
    if perf_bin.is_some() && syscalls != SyscallMethod::Perf {
        return Err(format!(
            "--syscalls {} does not use --perf-bin; pass only the selected counter's path",
            method_label(syscalls)
        ));
    }
    if strace_bin.is_some() && syscalls != SyscallMethod::Strace {
        return Err(format!(
            "--syscalls {} does not use --strace-bin; pass only the selected counter's path",
            method_label(syscalls)
        ));
    }
    match syscalls {
        SyscallMethod::Perf if perf_bin.is_none() => {
            Err("--syscalls perf requires --perf-bin".to_string())
        }
        SyscallMethod::Strace if strace_bin.is_none() => {
            Err("--syscalls strace requires --strace-bin".to_string())
        }
        _ => Ok(()),
    }
}

/// Read every `*.json` directly in `out_dir` as a `RunRecord`.
/// `doc::load_records` (task 12) delegates to this. A file that does
/// not parse is fatal: a partial record set is worse than no resume.
/// A file whose name is not the record's own `file_name()` is fatal
/// too: a copied or renamed record would otherwise count as a
/// repetition that never ran.
pub fn read_records(dir: &Path) -> Result<Vec<RunRecord>, String> {
    let mut out = Vec::new();
    let entries =
        fs::read_dir(dir).map_err(|e| format!("read dir {}: {e}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("entry: {e}"))?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("json") {
            let file = fs::File::open(&path)
                .map_err(|e| format!("open {}: {e}", path.display()))?;
            let rec: RunRecord = serde_json::from_reader(file)
                .map_err(|e| format!("parse {}: {e}", path.display()))?;
            let expected = rec.file_name();
            if path.file_name().and_then(|n| n.to_str()) != Some(expected.as_str()) {
                return Err(format!(
                    "{}: file name does not match its record, which names {expected}",
                    path.display()
                ));
            }
            out.push(rec);
        }
    }
    Ok(out)
}

/// Refuse a `--resume` whose existing records do not match the new
/// run's environment (AC-63). The comparison is exactly the fields the
/// `check_resume` test lists: `schema`, `profile`, `git_sha`, `kernel`,
/// `host`, `cpu_model`, `tools`, `cpu_sets.server`,
/// `cpu_sets.generator`. `cpu_model` and `tools` are compared because
/// `doc::render` refuses a recorded directory holding more than one
/// value of either (`MixedEnvironment("cpu_model")` /
/// `MixedEnvironment("tools")`): a resume that accepted a changed
/// `--perf-bin`, a changed tool version or a changed CPU model would
/// schedule hours of new attempts whose combined directory the renderer
/// then refuses to publish. The selected `syscalls` method is compared
/// against every counted record for the same reason: `Env.tools` lists
/// every supplied counter path, so a `--syscalls perf` session and a
/// `--syscalls strace` resume of it share one environment, and
/// `doc::render` then refuses the published mix
/// (`mixed syscall counter methods`). Only records that carry counter
/// evidence are compared — every kind but the 80 % latency run records
/// method `None` (`bench/src/runner.rs`). Each record is then
/// recomputed, because every later reader trusts its stored summary: a
/// record whose `valid` or `throughput_msgs_per_sec` no longer follows
/// from its own raw observations decides `resume_plan` and, through
/// `passing_records_for_key`, the matched rate the new latency runs are
/// paced at. `finalize_record` derives every one of those fields from
/// the raw data it just collected, so a mismatch is an edit made outside
/// the harness, not a run this directory ever produced. The first record
/// that disagrees ends the check, and every rejection names its file so
/// an operator can find it among a full matrix's records.
pub fn check_resume(
    existing: &[RunRecord],
    profile: Profile,
    syscalls: SyscallMethod,
    env: &Env,
    cpu_sets: &CpuSets,
) -> Result<(), String> {
    for rec in existing {
        let file = rec.file_name();
        if rec.schema != SCHEMA_VERSION {
            return Err(format!(
                "{file}: record schema {} != {}",
                rec.schema, SCHEMA_VERSION
            ));
        }
        if rec.profile != profile {
            return Err(format!(
                "{file}: record profile {:?} != {:?}",
                rec.profile, profile
            ));
        }
        if rec.env.git_sha != env.git_sha {
            return Err(format!(
                "{file}: env.git_sha {} != {}",
                rec.env.git_sha, env.git_sha
            ));
        }
        if rec.env.kernel != env.kernel {
            return Err(format!(
                "{file}: env.kernel {} != {}",
                rec.env.kernel, env.kernel
            ));
        }
        if rec.env.host != env.host {
            return Err(format!(
                "{file}: env.host {} != {}",
                rec.env.host, env.host
            ));
        }
        if rec.env.cpu_model != env.cpu_model {
            return Err(format!(
                "{file}: env.cpu_model {} != {}",
                rec.env.cpu_model, env.cpu_model
            ));
        }
        if rec.env.tools != env.tools {
            return Err(format!(
                "{file}: env.tools {} != {}",
                format_tools(&rec.env.tools),
                format_tools(&env.tools)
            ));
        }
        if rec.syscalls.method != SyscallMethod::None && rec.syscalls.method != syscalls {
            return Err(format!(
                "{file}: record syscalls.method {} != --syscalls {}",
                method_label(rec.syscalls.method),
                method_label(syscalls)
            ));
        }
        if rec.cpu_sets.server != cpu_sets.server {
            return Err(format!(
                "{file}: cpu_sets.server {} != {}",
                rec.cpu_sets.server, cpu_sets.server
            ));
        }
        if rec.cpu_sets.generator != cpu_sets.generator {
            return Err(format!(
                "{file}: cpu_sets.generator {} != {}",
                rec.cpu_sets.generator, cpu_sets.generator
            ));
        }
        let mismatches = recompute(rec);
        if !mismatches.is_empty() {
            return Err(format!(
                "{file}: stored summary disagrees with the raw observations: {}",
                mismatches.join("; ")
            ));
        }
    }
    Ok(())
}

/// Name a syscall counter method as the `--syscalls` value that selects
/// it, so a refusal reads in the operator's own CLI terms.
fn method_label(method: SyscallMethod) -> &'static str {
    match method {
        SyscallMethod::Perf => "perf",
        SyscallMethod::Strace => "strace",
        SyscallMethod::None => "none",
    }
}

/// Render `Env.tools` as `path=version` pairs for a refusal message, so
/// an operator sees which probed tool changed rather than a debug dump.
fn format_tools(tools: &[(String, String)]) -> String {
    if tools.is_empty() {
        return "none".to_string();
    }
    tools
        .iter()
        .map(|(path, version)| format!("{path}={version}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Compute the resume decision per configuration. Only a complete
/// attempt ([`attempt_complete`] — repetitions `1..=repetitions` each
/// present once, none marked `cancelled: true`) counts against
/// `max_config_attempts`: an interrupted attempt stays on disk and
/// costs nothing, so `remaining = max_config_attempts - completed` is
/// what `run_configs` charges against. The highest attempt decides
/// whether the configuration is `Passed`; otherwise `attempt` advances
/// past every recorded attempt so no file name or scratch directory is
/// reused. A configuration whose highest recorded attempt is
/// `u32::MAX - 1` advances to `u32::MAX` and `run_configs` still
/// dispatches that number once, so the operator sees the failure in
/// `MatrixOutcome::failed` rather than a silent skip. One whose highest
/// recorded attempt already is `u32::MAX` cannot advance at all: it
/// keeps that number with `remaining: 0`, which dispatches nothing and
/// leaves the configuration failed, because the only number left to run
/// is one whose files are already on disk.
pub fn resume_plan(
    existing: &[RunRecord],
    constants: &ProtocolConstants,
) -> HashMap<ConfigKey, ResumeState> {
    let mut by_key: HashMap<ConfigKey, Vec<RunRecord>> = HashMap::new();
    for rec in existing {
        by_key.entry(rec.key()).or_default().push(rec.clone());
    }
    let mut plan = HashMap::new();
    for (key, mut runs) in by_key {
        runs.sort_by_key(|r| r.attempt);
        let highest = runs.last().map_or(0, |r| r.attempt);
        let max_attempt_runs: Vec<RunRecord> = runs
            .iter()
            .filter(|r| r.attempt == highest)
            .cloned()
            .collect();
        let mut groups: HashMap<u32, Vec<RunRecord>> = HashMap::new();
        for rec in &runs {
            groups.entry(rec.attempt).or_default().push(rec.clone());
        }
        let completed = u32::try_from(
            groups
                .values()
                .filter(|g| attempt_complete(g, constants.repetitions))
                .count(),
        )
        .unwrap_or(u32::MAX);
        if config_passes(&max_attempt_runs, constants.repetitions, constants.enforce_spread) {
            plan.insert(key, ResumeState::Passed);
        } else {
            match highest.checked_add(1) {
                Some(next) => {
                    plan.insert(
                        key,
                        ResumeState::NextAttempt {
                            attempt: next,
                            remaining: constants.max_config_attempts.saturating_sub(completed),
                        },
                    );
                }
                None => {
                    plan.insert(
                        key,
                        ResumeState::NextAttempt {
                            attempt: highest,
                            remaining: 0,
                        },
                    );
                }
            }
        }
    }
    plan
}

/// `specs` are the RunSpec's the matrix dispatched (one per
/// `(server, workers, qos, kind, rate_fraction_pct)`); `run` is the
/// one-run executor (the matrix passes `runner::run_one` in
/// production; the test injects a stub). The function deduplicates
/// the specs by `ConfigKey`, then runs rounds of
/// `interleave(pending, repetitions)` until every config has passed or
/// has used its last attempt. Each config carries its own attempt
/// number — a resumed config starts at the attempt `resume_plan` gave
/// it — so two configs resumed at different attempts never collide on
/// a record filename. `remaining` caps each config at the completed
/// attempts it has left (decision Q9: three per configuration); a key
/// it does not list gets `constants.max_config_attempts`. Every round
/// that finishes completes one attempt per active config; an
/// interrupted round returns `Err` and completes none. Every record is
/// written to `out_dir/<file_name>`; failed attempts stay on disk.
/// `cancel` is read before every dispatch, so an interruption starts
/// nothing further. It charges no configuration an attempt at all: the
/// attempt it cut short is not a completed one ([`attempt_complete`]),
/// and only completed attempts count against the budget, so every
/// configuration's budget stays available to the `--resume` that
/// follows. `scratch_dir` is the runner's scratch root,
/// needed only to drop the scratch directory of a run whose record
/// could not be persisted. An attempt number whose files already
/// exist ([`attempt_in_use`]) is skipped at no cost, and the scratch
/// directory is dropped only when the run created it itself
/// ([`created_own_scratch`]), so a run never removes another run's
/// evidence.
pub fn run_configs<F: FnMut(&RunSpec) -> RunRecord, S: ::std::hash::BuildHasher>(
    specs: &[RunSpec],
    remaining: &HashMap<ConfigKey, u32, S>,
    constants: &ProtocolConstants,
    out_dir: &Path,
    scratch_dir: &Path,
    cancel: &AtomicBool,
    mut run: F,
) -> Result<MatrixOutcome, String> {
    fs::create_dir_all(out_dir).map_err(|e| format!("create out dir: {e}"))?;
    // Deduplicate specs by key, preserving the first occurrence of each.
    let mut spec_for_key: HashMap<ConfigKey, RunSpec> = HashMap::new();
    let mut pending_keys: Vec<ConfigKey> = Vec::new();
    for spec in specs {
        let key = key_of(spec);
        if let std::collections::hash_map::Entry::Vacant(slot) = spec_for_key.entry(key) {
            slot.insert(spec.clone());
            pending_keys.push(key);
        }
    }
    let mut next_attempt: HashMap<ConfigKey, u32> = spec_for_key
        .iter()
        .map(|(key, spec)| (*key, spec.attempt.max(1)))
        .collect();
    let mut left: HashMap<ConfigKey, u32> = spec_for_key
        .keys()
        .map(|key| {
            (
                *key,
                remaining.get(key).copied().unwrap_or(constants.max_config_attempts),
            )
        })
        .collect();

    let mut passed: Vec<ConfigKey> = Vec::new();

    loop {
        let mut active_specs: Vec<RunSpec> = Vec::new();
        for key in &pending_keys {
            if *left.get(key).unwrap_or(&0) == 0 {
                continue;
            }
            let Some(attempt) = next_attempt.get(key).copied() else {
                continue;
            };
            let Some(template) = spec_for_key.get(key).cloned() else {
                continue;
            };
            let mut spec = template;
            spec.attempt = attempt;
            while let Some(occupied) =
                attempt_in_use(&spec, constants.repetitions, out_dir, scratch_dir)?
            {
                // Name the file that pushed the attempt number on. A run
                // that lands on attempt 3 after skipping 1 and 2 looks
                // like two lost attempts otherwise, and reconstructing an
                // interrupted session needs the files that caused the jump
                // (AC-62).
                eprintln!(
                    "skipping attempt {} of {:?}: {} exists",
                    spec.attempt,
                    key,
                    occupied.display()
                );
                match spec.attempt.checked_add(1) {
                    Some(next) => spec.attempt = next,
                    None => {
                        if let Some(slot) = left.get_mut(key) {
                            *slot = 0;
                        }
                        break;
                    }
                }
            }
            if *left.get(key).unwrap_or(&0) == 0 {
                continue;
            }
            if let Some(slot) = next_attempt.get_mut(key) {
                *slot = spec.attempt;
            }
            active_specs.push(spec);
        }
        if active_specs.is_empty() {
            break;
        }
        let mut round: Vec<RunRecord> = Vec::new();
        for (mut spec, rep) in interleave(&active_specs, constants.repetitions) {
            // Stop scheduling the moment SIGINT/SIGTERM arrives. Dispatching
            // the rest of the round would write a cancellation record for
            // every remaining repetition and attempt, and each of those
            // records would complete an attempt that never really ran — an
            // interruption would cost the configuration its whole budget
            // (AC-52, AC-62).
            if cancel.load(Ordering::SeqCst) {
                return Err("cancelled".to_string());
            }
            // The interleave pairs the spec with its repetition number;
            // propagate that into the spec so the runner's `stem_for`
            // (which reads `spec.repetition` to build the per-run scratch
            // directory) names each repetition's directory distinctly.
            spec.repetition = rep;
            let mut record = run(&spec);
            // A signal that arrived while the run was in progress may have
            // cut it short: mark it, keep it on disk, and stop scheduling.
            let cancelled = cancel.load(Ordering::SeqCst);
            record.cancelled = cancelled;
            if let Err(e) = write_record(out_dir, &record) {
                if created_own_scratch(&record) {
                    discard_scratch(scratch_dir, &record);
                }
                return Err(e);
            }
            if cancelled {
                return Err("cancelled".to_string());
            }
            round.push(record);
        }
        // Group this round's records by key.
        let mut by_key: HashMap<ConfigKey, Vec<RunRecord>> = HashMap::new();
        for rec in round {
            by_key.entry(rec.key()).or_default().push(rec);
        }
        let mut still_pending = Vec::new();
        for key in &pending_keys {
            // A key whose attempts are exhausted ran nothing this round
            // and stays pending until every other key is exhausted too.
            let Some(runs) = by_key.get(key) else {
                still_pending.push(*key);
                continue;
            };
            match config_failure(runs, constants.repetitions, constants.enforce_spread) {
                None => passed.push(*key),
                Some(reason) => {
                    // Name the gate condition this attempt failed. Once the
                    // budget is spent the caller reports the configuration
                    // key alone, so without this line the operator has to
                    // re-derive the verdict from the records themselves.
                    eprintln!(
                        "attempt {} of {:?} failed: {reason}",
                        runs.first().map_or(0, |r| r.attempt),
                        key
                    );
                    match next_attempt.get_mut(key).and_then(|a| a.checked_add(1)) {
                        Some(n) => {
                            if let Some(slot) = left.get_mut(key) {
                                *slot = slot.saturating_sub(1);
                            }
                            if let Some(slot) = next_attempt.get_mut(key) {
                                *slot = n;
                            }
                        }
                        None => {
                            if let Some(slot) = left.get_mut(key) {
                                *slot = 0;
                            }
                        }
                    }
                    still_pending.push(*key);
                }
            }
        }
        pending_keys = still_pending;
    }

    Ok(MatrixOutcome {
        passed,
        failed: pending_keys,
    })
}

/// Persist one record as `out_dir/<file_name>`. The JSON is serialized
/// into a sibling `.tmp` path created with `create_new` — it never
/// follows a symlink and never truncates — and `sync_all` brings its
/// bytes onto disk before `hard_link` publishes the record; a planted
/// `<name>.json.tmp` (file or symlink) or an existing `<name>.json`
/// stops the write with a named error and is left untouched, and only
/// the temporary file this call created itself is removed on failure.
fn write_record(out_dir: &Path, record: &RunRecord) -> Result<(), String> {
    let name = record.file_name();
    let tmp = out_dir.join(format!("{name}.tmp"));
    let path = out_dir.join(&name);
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                format!(
                    "temporary record {} already exists; refusing to write through it",
                    tmp.display()
                )
            } else {
                format!("create {}: {e}", tmp.display())
            }
        })?;
    let mut writer = std::io::BufWriter::new(file);
    if let Err(e) = serde_json::to_writer_pretty(&mut writer, record) {
        drop(writer);
        let _ = fs::remove_file(&tmp);
        return Err(format!("write {}: {e}", tmp.display()));
    }
    if let Err(e) = writer.flush() {
        drop(writer);
        let _ = fs::remove_file(&tmp);
        return Err(format!("flush {}: {e}", tmp.display()));
    }
    let file = writer
        .into_inner()
        .map_err(|e| format!("flush {}: {e}", tmp.display()))?;
    if let Err(e) = file.sync_all() {
        drop(file);
        let _ = fs::remove_file(&tmp);
        return Err(format!("sync {}: {e}", tmp.display()));
    }
    drop(file);
    let linked = fs::hard_link(&tmp, &path);
    let cleaned = fs::remove_file(&tmp);
    match linked {
        Ok(()) => {
            if let Err(e) = cleaned {
                // The record is published: the hard link succeeded, so the
                // write is not retried and the run's scratch evidence is
                // not discarded. Only the temporary name stayed behind, and
                // `attempt_in_use` reads a leftover `.tmp` as an occupied
                // attempt number, so the operator has to hear about it.
                eprintln!("remove {}: {e}", tmp.display());
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(format!(
            "record {} already exists; refusing to overwrite it",
            path.display()
        )),
        Err(e) => Err(format!(
            "link {} to {}: {e}",
            tmp.display(),
            path.display()
        )),
    }
}

/// Drop the scratch directory of a run whose record could not be
/// persisted. `run_one` refuses a spec whose scratch directory already
/// exists, so the orphan left by a failed write would make that attempt
/// impossible to repeat: the next run finds no record for the key,
/// dispatches the same attempt, and gets `scratch directory reused`
/// forever. The raw evidence in there has no consumer without its
/// record — `read_records` and `doc::load_records` only read `*.json` —
/// so removing it is what keeps the attempt recoverable once the write
/// error (a full filesystem, say) is repaired. The caller must prove
/// the run owns that directory ([`created_own_scratch`]) first.
fn discard_scratch(scratch_dir: &Path, record: &RunRecord) {
    let stem = record.file_name().replace(".json", "");
    let _ = fs::remove_dir_all(scratch_dir.join(stem));
}

/// `true` when `record`'s run created its own scratch directory, which
/// is what licences [`discard_scratch`] to remove it. [`attempt_in_use`]
/// skips an attempt number whose files are already there, but it cannot
/// stop a second process that inspected the same empty output directory
/// before either run created anything: both pass the check, one wins
/// the exclusive `create_dir` in `runner::run_one`, and the loser's
/// record carries [`SCRATCH_REUSED_PREFIX`]. That loser's own record
/// write then collides with the winner's published one, so without this
/// proof its cleanup would delete the winner's raw evidence.
fn created_own_scratch(record: &RunRecord) -> bool {
    !record
        .harness_errors
        .iter()
        .any(|e| e.starts_with(SCRATCH_REUSED_PREFIX))
}

/// The first file any repetition of `spec`'s attempt already left
/// behind: a record, a temporary record, or a scratch directory.
/// `None` when the attempt number is free. Such an attempt number
/// belongs to an earlier run — one cut short before its record was
/// published, say — so dispatching it would collide with that run's
/// files. The occupying path is returned rather than a bare `true` so
/// the caller's skip diagnostic can name what pushed the attempt
/// number on. `symlink_metadata` counts a dangling symlink as present;
/// only `NotFound` counts as absent, and any other inspection error is
/// returned, because unknown occupancy is no licence to dispatch.
fn attempt_in_use(
    spec: &RunSpec,
    repetitions: u32,
    out_dir: &Path,
    scratch_dir: &Path,
) -> Result<Option<PathBuf>, String> {
    for rep in 1..=repetitions {
        let mut planned = spec.clone();
        planned.repetition = rep;
        let stem = stem_for(&planned);
        for path in [
            out_dir.join(format!("{stem}.json")),
            out_dir.join(format!("{stem}.json.tmp")),
            scratch_dir.join(&stem),
        ] {
            match fs::symlink_metadata(&path) {
                Ok(_) => return Ok(Some(path)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(format!("inspect {}: {e}", path.display())),
            }
        }
    }
    Ok(None)
}

/// Drive the §9.1 matrix end to end. Each `(workers, qos)` pair runs
/// its throughput configurations first, then latency configurations at
/// 50 % and 80 % of the slowest passing throughput rate, then (after
/// every QoS of one `w`) connections configurations. When a syscall
/// counter is selected, the open-loop stage also dispatches the counted
/// configuration at the matched 80 % rate, interleaved with the latency
/// runs. A failed throughput row marks all of them failed without
/// running. SIGINT/SIGTERM cancellation is observed before every `run`
/// call: once set, `run_matrix_with` returns `Err("cancelled")` after
/// the current run finishes. `run` is the one-run executor; the
/// production caller passes `|s| crate::runner::run_one(s, host)` and
/// the tests pass a stub that observes the dispatched specs without
/// spawning a runner.
pub fn run_matrix_with<F: FnMut(&RunSpec) -> RunRecord>(
    spec: &MatrixSpec,
    host: &HostSetup,
    mut run: F,
) -> Result<MatrixOutcome, String> {
    let constants = ProtocolConstants::for_profile(spec.profile);
    fs::create_dir_all(&spec.out_dir)
        .map_err(|e| format!("create out dir: {e}"))?;
    fs::create_dir_all(&host.scratch_dir)
        .map_err(|e| format!("create scratch: {e}"))?;

    // Resume: refuse mismatched env, build the plan, capture records so
    // the matched-rate calculation can read them.
    let (plan, mut existing_records) = if spec.resume {
        let existing = read_records(&spec.out_dir)?;
        check_resume(
            &existing,
            spec.profile,
            host.syscalls,
            &host.env,
            &host.cpu_sets,
        )?;
        let plan = resume_plan(&existing, &constants);
        (plan, existing)
    } else {
        (HashMap::new(), Vec::new())
    };
    let remaining: HashMap<ConfigKey, u32> = plan
        .iter()
        .filter_map(|(key, state)| match state {
            ResumeState::NextAttempt { remaining, .. } => Some((*key, *remaining)),
            ResumeState::Passed => None,
        })
        .collect();

    let mut outcome = MatrixOutcome::default();

    for &w in &spec.workers {
        let servers = servers_for(w, &spec.servers);

        for &q in &spec.qos {
            if host.cancel.load(Ordering::SeqCst) {
                return Err("cancelled".to_string());
            }
            // Throughput configurations.
            let mut thr_specs: Vec<RunSpec> = Vec::new();
            let mut thr_skipped: Vec<ConfigKey> = Vec::new();
            for &server in &servers {
                let mut s = RunSpec {
                    profile: spec.profile,
                    server,
                    workers: w,
                    qos: Some(q),
                    kind: RunKind::Throughput,
                    rate_fraction_pct: None,
                    offered_rate: None,
                    repetition: 0,
                    attempt: 1,
                };
                if plan_spec(&plan, &mut s) {
                    thr_specs.push(s);
                } else {
                    thr_skipped.push(key_of(&s));
                }
            }
            let thr_outcome = run_configs(
                &thr_specs,
                &remaining,
                &constants,
                &spec.out_dir,
                &host.scratch_dir,
                &host.cancel,
                &mut run,
            )?;
            let thr_failed_keys = thr_outcome.failed;
            // A config the resume plan skipped already passed, so its
            // existing records feed the matched-rate median.
            let mut thr_passed_keys = thr_outcome.passed;
            thr_passed_keys.extend(thr_skipped);
            outcome.passed.extend(thr_passed_keys.iter().copied());
            outcome.failed.extend(thr_failed_keys.iter().copied());

            // Reload the records we just wrote so we can read the
            // per-server throughput metric for matched_rates.
            if spec.resume {
                // On resume we add the new records to the existing list
                // for the matched-rate calculation; the highest-attempt
                // passing records are already in `existing_records`.
                let new_records = read_records(&spec.out_dir)?;
                existing_records = merge_records(existing_records, new_records);
            } else {
                existing_records = read_records(&spec.out_dir)?;
            }

            if !thr_failed_keys.is_empty() {
                // Mark latency configs as failed (unrun).
                for &server in &servers {
                    for &pct in &LATENCY_FRACTIONS_PCT {
                        outcome.failed.push(ConfigKey {
                            server,
                            workers: w,
                            qos: Some(q),
                            kind: RunKind::Latency,
                            rate_fraction_pct: Some(pct),
                        });
                    }
                    if host.syscalls != SyscallMethod::None {
                        outcome.failed.push(ConfigKey {
                            server,
                            workers: w,
                            qos: Some(q),
                            kind: RunKind::Counted,
                            rate_fraction_pct: Some(SYSCALL_FRACTION_PCT),
                        });
                    }
                }
                continue;
            }

            // Compute matched rates from the passing throughput records.
            let throughput_medians: Vec<f64> = thr_passed_keys
                .iter()
                .filter_map(|key| {
                    median(&passing_records_for_key(&existing_records, *key)
                        .iter()
                        .filter_map(crate::analysis::metric)
                        .collect::<Vec<f64>>())
                })
                .collect();
            let rates = matched_rates(&throughput_medians).unwrap_or((0, 0));
            let (rate_50, rate_80) = rates;

            // Latency configurations.
            let mut lat_specs: Vec<RunSpec> = Vec::new();
            for &server in &servers {
                for &pct in &LATENCY_FRACTIONS_PCT {
                    let rate = if pct == 50 { rate_50 } else { rate_80 };
                    let mut s = RunSpec {
                        profile: spec.profile,
                        server,
                        workers: w,
                        qos: Some(q),
                        kind: RunKind::Latency,
                        rate_fraction_pct: Some(pct),
                        offered_rate: Some(rate),
                        repetition: 0,
                        attempt: 1,
                    };
                    if plan_spec(&plan, &mut s) {
                        lat_specs.push(s);
                    } else {
                        outcome.passed.push(key_of(&s));
                    }
                }
                if host.syscalls != SyscallMethod::None {
                    let mut s = RunSpec {
                        profile: spec.profile,
                        server,
                        workers: w,
                        qos: Some(q),
                        kind: RunKind::Counted,
                        rate_fraction_pct: Some(SYSCALL_FRACTION_PCT),
                        offered_rate: Some(rate_80),
                        repetition: 0,
                        attempt: 1,
                    };
                    if plan_spec(&plan, &mut s) {
                        lat_specs.push(s);
                    } else {
                        outcome.passed.push(key_of(&s));
                    }
                }
            }
            let lat_outcome = run_configs(
                &lat_specs,
                &remaining,
                &constants,
                &spec.out_dir,
                &host.scratch_dir,
                &host.cancel,
                &mut run,
            )?;
            outcome.passed.extend(lat_outcome.passed);
            outcome.failed.extend(lat_outcome.failed);
        }

        if host.cancel.load(Ordering::SeqCst) {
            return Err("cancelled".to_string());
        }
        // Connections configurations.
        let mut conn_specs: Vec<RunSpec> = Vec::new();
        for &server in &servers {
            let mut s = RunSpec {
                profile: spec.profile,
                server,
                workers: w,
                qos: None,
                kind: RunKind::Connections,
                rate_fraction_pct: None,
                offered_rate: None,
                repetition: 0,
                attempt: 1,
            };
            if plan_spec(&plan, &mut s) {
                conn_specs.push(s);
            } else {
                outcome.passed.push(key_of(&s));
            }
        }
        let conn_outcome = run_configs(
            &conn_specs,
            &remaining,
            &constants,
            &spec.out_dir,
            &host.scratch_dir,
            &host.cancel,
            &mut run,
        )?;
        outcome.passed.extend(conn_outcome.passed);
        outcome.failed.extend(conn_outcome.failed);
    }

    Ok(outcome)
}

/// [`run_matrix_with`] over [`runner::run_one`](crate::runner::run_one).
pub fn run_matrix(spec: &MatrixSpec, host: &HostSetup) -> Result<MatrixOutcome, String> {
    run_matrix_with(spec, host, |s| crate::runner::run_one(s, host))
}

fn key_of(spec: &RunSpec) -> ConfigKey {
    ConfigKey {
        server: spec.server,
        workers: spec.workers,
        qos: spec.qos,
        kind: spec.kind,
        rate_fraction_pct: spec.rate_fraction_pct,
    }
}

/// Apply the `--resume` plan to one spec. `false` means the config does
/// not run: its highest attempt already passed, and running it again
/// would reuse the scratch directory `run_one` refuses, so the invalid
/// record it returned would overwrite the passing JSON under the same
/// filename. A `NextAttempt { attempt, .. }` config runs with
/// `spec.attempt = attempt`.
fn plan_spec(plan: &HashMap<ConfigKey, ResumeState>, spec: &mut RunSpec) -> bool {
    match plan.get(&key_of(spec)) {
        Some(ResumeState::Passed) => false,
        Some(ResumeState::NextAttempt { attempt, .. }) => {
            spec.attempt = *attempt;
            true
        }
        None => true,
    }
}

/// The records of a passing config's highest attempt. A config passes on
/// its last attempt, so every earlier attempt was rejected for spread or
/// validity and must not enter the matched-rate median.
fn passing_records_for_key(records: &[RunRecord], key: ConfigKey) -> Vec<RunRecord> {
    let mut runs: Vec<RunRecord> =
        records.iter().filter(|r| r.key() == key).cloned().collect();
    let highest = runs.iter().map(|r| r.attempt).max().unwrap_or(0);
    runs.retain(|r| r.attempt == highest);
    runs
}

fn out_dir_has_json(dir: &Path) -> Result<bool, String> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(format!("read out dir: {e}")),
    };
    let mut has_json = false;
    for entry in entries {
        let entry = entry.map_err(|e| format!("read out dir entry: {e}"))?;
        if entry.path().extension().and_then(|e| e.to_str()) == Some("json") {
            has_json = true;
        }
    }
    Ok(has_json)
}

fn merge_records(a: Vec<RunRecord>, b: Vec<RunRecord>) -> Vec<RunRecord> {
    let mut out = a;
    for rec in b {
        let key = (rec.key(), rec.attempt, rec.repetition);
        if !out
            .iter()
            .any(|r| (r.key(), r.attempt, r.repetition) == key)
        {
            out.push(rec);
        }
    }
    out
}
