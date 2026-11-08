# F4.3 diagnosis: generator placement and the 4-worker open-loop shortfall

This file is the evidence of the attended diagnosis session that
`features/bench-worker-loads-record/RESEARCH.md` `## Decisions` Q4-Q8 pre-registered. Experiment 1
is the generator-placement A/B (Q4-Q6). Experiment 2 is the uring 4-worker open-loop shortfall
diagnosis (Q7, Q8). The verdicts below apply the pre-registered rules only. A follow-up `/prep`
reads this file and decides the fix shape and the `## Method` disclosure. The `/epic` lateness ruling
(EPIC-PLAN `## Remaining to close` row 2) reads the lateness evidence.

## Verdicts

- **Experiment 1 (Q6): robust.** No configuration differs between the two placement arms under the
  Q6 rule. This result covers only the two pre-registered arms: see "Scope of experiment 1".
- **Experiment 2 (Q7, Q8): unexplained.** The rule holds in 38 of the 61 invalid 4-worker runs
  (uring 17 of 28, tokio twin 21 of 33). The rule requires all 61. The data is kept below.

## Method

- **Source:** commit `d637d09` (the `main` HEAD on 2026-11-07; `git log d637d09..main -- bench/ src/`
  was empty) in a scratch `git worktree` (`/home/alex/dev/7laps/uring-mqtt-diag`, detached), with the
  diagnosis-only patch below. The patch is not committed. No file under `src/` or `bench/src/` on
  `main` changed.
- **What the patch does:** under `bench run --profile smoke`, the env var `DIAG_RATES` switches the
  smoke timing to the recorded timing (10 s warm-up, 60 s window, 5 s drain), sets the repetition
  count from `DIAG_REPS`, sets one attempt per configuration, turns off the spread check, and skips
  the throughput and connections stages. `DIAG_RATES=declared` dispatches the recorded profile's
  declared rates for each row group. A comma-separated list replaces each row's latency rates. Every
  run goes through the unmodified `run_one`, `reconcile` and record writer, so each record reconciles
  exactly as a recorded run would. No §9.1 limit (reconciliation, lateness, steady state), no
  declared rate and no `SPREAD_LIMIT` changed.
- **Consequences of the patch in the records:** every record carries `"profile": "smoke"`. With one
  attempt and no spread check, an invalid run is kept and the session continues. Each `bench run`
  therefore exited 2 (`bench/src/matrix.rs:118-124`: failed configurations), and its stderr names
  each failed configuration as `smoke harness-health failure`. No `bench run` exited 1 (refusal).
  No record is cancelled and no record carries a harness error.
- **Build:** release (`cargo build --release --manifest-path bench/Cargo.toml` in the worktree). The
  binary was first built on 2026-10-28 at 17:58. The pre-check on 2026-11-07 at 00:58 recompiled the
  `bench` crate (6.5 s) from the same patched tree, and the binary size stayed 4 746 552 bytes. All
  240 runs used that 2026-11-07 binary.
- **Host:** the owner's WSL2 workstation, Intel Core i7-14650HX, 18 logical CPUs (9 guest cores × 2
  threads, hypervisor vendor Microsoft), kernel `6.18.33.2-microsoft-standard-WSL2`, rustc
  `1.100.0-nightly (17fd5b8a3 2026-08-28)`, mosquitto 2.0.18 (`/usr/sbin/mosquitto`). Before the
  start, `mosquitto.service` was inactive, port 1883 was free, the host was on AC power (battery
  full), and `/proc/pressure/cpu` `some avg10` was 0.00. The owner confirmed that the machine stayed
  idle for the session.
- **Server CPUs:** `0,2,4,6` in every run. Flags: `--servers`, `--workers`, `--qos` per step,
  `--syscalls none`, `--mosquitto-bin /usr/sbin/mosquitto`, `--server-cpus 0,2,4,6`,
  `--generator-cpus <arm>`. The driver script is in the appendix.
- **Dates and times (local, +03:00):** 2026-11-07, 01:08:24-06:02:16. Experiment 1 ran
  01:08:24-05:13:31. Experiment 2 ran 05:13:31-06:02:16.
- **Pressure gate:** before each of the 11 `bench run` steps, the driver waited until
  `/proc/pressure/cpu` `some avg10` was below 2 %. Every step passed at the first check, with
  values 0.00-1.92. Host CPU pressure over each run is in the tables (0.2-2.6 %; at most 2.0 % at
  1 worker). Pressure is evidence only (Decision log 2026-10-18): no rule here uses it.
- **Discarded attempt:** a first attempt started on 2026-10-28 at 18:02 and was stopped after 9 runs,
  because the owner needed the machine for parallel work. Its records were contaminated and were
  deleted before this session. No record of that attempt is in this file or under `bench/diagnosis/`.
- **`env.git_dirty` is true** in all 240 records because of the patch. `env.git_sha` is `d637d09` and
  `env` is identical in all 240 records.
- **Repetition numbering:** experiment 1 ran one `bench run` with `DIAG_REPS=1` per (arm, repetition)
  into `bench/diagnosis/exp1/<arm>/rep<k>/`. Each experiment 1 record therefore carries
  `"repetition": 1`, and the directory name gives the repetition. `analyze.py` reads the repetition
  from the directory. Experiment 2 ran in one `bench run` with `DIAG_REPS=5`, so its records carry
  repetitions 1-5.
- **Order:** experiment 1 interleaved the arms per repetition with alternating order: rep 1 A then B,
  rep 2 B then A, rep 3 A then B, rep 4 B then A, rep 5 A then B. Each `bench run` covered every
  declared rate (w1 q1 75 000 and 300 000, w1 q0 150 000 and 400 000, w4 q1 900 000 and 1 200 000,
  w4 q0 1 300 000 and 1 500 000 msgs/s) for uring and the tokio twin, and mosquitto at 1 worker only:
  20 runs per invocation, 200 runs in total. Experiment 2 ran uring and the tokio twin at 4 workers
  QoS 1 at 1 300 000, 1 500 000, 1 700 000 and 2 000 000 msgs/s, 5 repetitions, placement `8-17`:
  40 runs.
- **Analysis:** `python3 -I analyze.py bench/diagnosis` with the `analyze.py` in the appendix. The
  2026-10-28 session note records that it was written before any data. Its file time (2026-10-28
  18:13:58, the same for every file of the scripts directory) is after the 18:02 start of the
  discarded attempt, so the file system alone cannot prove that order. It was not changed in this
  session. It reported 240
  records (experiment 1: 200, experiment 2: 40), no arm mismatch, and no cancelled run or harness
  error. The per-configuration lateness summary and the experiment 2 per-configuration table come
  from two small evidence-only scripts (appendix). They apply no rule.

## Placement arms

`lscpu -e` in the guest reports CPU pairs 0/1, 2/3, 4/5, 6/7, 8/9, 10/11, 12/13, 14/15 and 16/17 on
one guest core each (appendix). The server CPUs `0,2,4,6` are one thread of guest cores 0-3.

| Arm | Generator CPU set | Guest cores used | Server-core SMT siblings in the set |
|---|---|---|---|
| A | `8-17` (recorded default) | 4-8, both threads | none |
| B | `1,3,5,7,8-13` | 0-3 (sibling thread of each server CPU), 4-6 both threads | all four: 1, 3, 5, 7 |

- Physical core sharing under Hyper-V is unverified. The guest-reported SMT pairs need not map to
  physical hyperthread pairs on the host.
- The generator footprint (CPU count) was not tested. Both arms keep 10 CPUs and 10 generator
  workers, because `bench/src/gen.rs:363` sizes the generator runtime by `available_parallelism`,
  which follows the CPU set.

## Experiment 1: per-configuration results

"Repetition p99" is the latency p99 of each repetition. "Lateness p99" is the generator-lateness
p99. "Pressure" is host CPU pressure over the run. "Failed rows" names the reconcile rows behind an
invalid run; derived count rows (samples, scheduled) are omitted, as in `bench/CALIBRATION.md`.
"Differs (Q6)" is the rule result for the configuration.

| Workers | QoS | Rate (msgs/s) | Server | Arm | Repetition p99 (ms), rep 1-5 | Valid | Lateness p99 (ms), rep 1-5 | Pressure (%), rep 1-5 | Failed rows (rep: rows) | Differs (Q6) |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 75000 | uring | A `8-17` | 0.709, 0.675, 0.694, 0.786, 0.870 | 4/5 | 0.067, 0.068, 0.068, 0.067, 0.075 | 0.2, 0.3, 0.2, 0.3, 0.2 | 2: lateness ratio | no |
| 1 | 1 | 75000 | uring | B `1,3,5,7,8-13` | 1.010, 0.860, 0.710, 0.723, 0.719 | 5/5 | 0.086, 0.071, 0.064, 0.063, 0.068 | 0.3, 0.3, 0.3, 0.3, 0.3 |  |  |
| 1 | 1 | 75000 | tokio | A `8-17` | 2.435, 0.793, 0.716, 0.926, 0.800 | 5/5 | 0.085, 0.063, 0.067, 0.070, 0.062 | 0.3, 0.2, 0.2, 0.2, 0.2 |  | no |
| 1 | 1 | 75000 | tokio | B `1,3,5,7,8-13` | 1.210, 1.321, 0.844, 0.804, 0.941 | 5/5 | 0.079, 0.077, 0.067, 0.064, 0.070 | 0.3, 0.2, 0.2, 0.2, 0.3 |  |  |
| 1 | 1 | 75000 | mosquitto | A `8-17` | 1661.993, 51.511, 117.768, 104.399, 2506.097 | 2/5 | 0.227, 0.097, 0.109, 0.124, 0.115 | 0.5, 0.2, 0.4, 0.2, 0.3 | 1: steady state; 3: steady state; 5: steady state | no |
| 1 | 1 | 75000 | mosquitto | B `1,3,5,7,8-13` | 55.738, 945.291, 13.730, 151.257, 61.178 | 5/5 | 0.125, 0.130, 0.156, 0.108, 0.121 | 0.4, 0.3, 0.3, 0.3, 0.4 |  |  |
| 1 | 1 | 300000 | uring | A `8-17` | 39.289, 29.409, 47.055, 71.565, 330.826 | 5/5 | 0.455, 0.381, 0.399, 0.417, 0.508 | 0.5, 0.4, 0.4, 0.4, 0.4 |  | no |
| 1 | 1 | 300000 | uring | B `1,3,5,7,8-13` | 91.685, 75.039, 81.461, 9.642, 17.957 | 5/5 | 0.478, 0.541, 0.378, 0.549, 0.432 | 0.4, 0.4, 0.4, 0.4, 0.3 |  |  |
| 1 | 1 | 300000 | tokio | A `8-17` | 492.044, 662.176, 453.509, 203.162, 855.638 | 4/5 | 0.451, 0.383, 0.434, 0.335, 0.333 | 0.4, 0.3, 0.3, 0.4, 0.3 | 4: steady state | no |
| 1 | 1 | 300000 | tokio | B `1,3,5,7,8-13` | 2686.452, 537.395, 57.573, 709.362, 601.883 | 4/5 | 7.012, 0.522, 0.537, 0.409, 0.510 | 0.6, 0.3, 0.4, 0.3, 0.4 | 1: steady state |  |
| 1 | 1 | 300000 | mosquitto | A `8-17` | 56405.000, 57914.950, 55868.129, 47110.423, 55197.041 | 0/5 | 32833.012, 4601.151, 2931.818, 4416.602, 8413.774 | 0.5, 0.3, 0.3, 0.3, 0.3 | 1: unresolved, missed slots, stalled, unacknowledged, lateness ratio, steady state; 2: unresolved, missed slots, stalled, unacknowledged, steady state; 3: unresolved, missed slots, stalled, unacknowledged, steady state; 4: unresolved, missed slots, stalled, unacknowledged, steady state; 5: unresolved, missed slots, stalled, unacknowledged, lateness ratio, steady state | no |
| 1 | 1 | 300000 | mosquitto | B `1,3,5,7,8-13` | 56304.337, 36507.222, 45097.157, 55230.595, 42412.802 | 0/5 | 5570.036, 5297.406, 2776.629, 1501.561, 2279.604 | 0.4, 0.3, 0.4, 0.4, 0.4 | 1: unresolved, missed slots, stalled, unacknowledged, steady state; 2: unresolved, missed slots, stalled, unacknowledged, lateness ratio, steady state; 3: unresolved, missed slots, stalled, unacknowledged, steady state; 4: unresolved, missed slots, stalled, unacknowledged, steady state; 5: unresolved, missed slots, stalled, unacknowledged, steady state |  |
| 1 | 0 | 150000 | uring | A `8-17` | 7.156, 4.665, 4.850, 4.702, 4.932 | 5/5 | 0.348, 0.186, 0.155, 0.180, 0.194 | 0.4, 0.3, 0.3, 0.3, 0.3 |  | no |
| 1 | 0 | 150000 | uring | B `1,3,5,7,8-13` | 4.891, 5.112, 5.063, 4.379, 5.140 | 5/5 | 0.335, 0.298, 0.181, 0.210, 0.231 | 0.4, 0.3, 0.3, 0.3, 0.3 |  |  |
| 1 | 0 | 150000 | tokio | A `8-17` | 4.764, 4.485, 5.009, 4.661, 4.387 | 5/5 | 0.175, 0.150, 0.171, 0.142, 0.144 | 0.3, 0.3, 0.3, 0.3, 0.2 |  | no |
| 1 | 0 | 150000 | tokio | B `1,3,5,7,8-13` | 4.801, 4.903, 4.563, 4.407, 4.432 | 5/5 | 0.230, 0.194, 0.143, 0.146, 0.162 | 0.6, 0.3, 0.2, 0.3, 0.2 |  |  |
| 1 | 0 | 150000 | mosquitto | A `8-17` | 128.451, 21.807, 77.595, 54.788, 93.716 | 5/5 | 0.149, 0.114, 0.164, 0.120, 0.142 | 0.3, 0.3, 0.3, 0.3, 0.3 |  | no |
| 1 | 0 | 150000 | mosquitto | B `1,3,5,7,8-13` | 86.180, 46.432, 357.564, 252.969, 2344.616 | 4/5 | 0.166, 0.150, 0.115, 0.101, 0.127 | 0.3, 0.3, 0.3, 0.2, 0.3 | 3: steady state |  |
| 1 | 0 | 400000 | uring | A `8-17` | 110.232, 21.922, 51.970, 16.908, 19.153 | 5/5 | 0.759, 0.652, 0.654, 0.852, 0.722 | 0.5, 0.4, 0.4, 0.4, 0.4 |  | no |
| 1 | 0 | 400000 | uring | B `1,3,5,7,8-13` | 50.102, 19.530, 26.706, 58.229, 17.072 | 5/5 | 1.528, 1.193, 1.033, 0.722, 0.895 | 2.0, 0.5, 0.5, 0.4, 0.4 |  |  |
| 1 | 0 | 400000 | tokio | A `8-17` | 91.685, 80.413, 89.915, 197.001, 155.976 | 5/5 | 0.786, 0.592, 0.542, 0.590, 0.637 | 0.4, 0.3, 0.3, 0.3, 0.3 |  | no |
| 1 | 0 | 400000 | tokio | B `1,3,5,7,8-13` | 70.779, 120.193, 157.417, 176.161, 140.640 | 5/5 | 1.281, 1.101, 0.670, 0.677, 0.760 | 0.6, 0.4, 0.4, 0.4, 0.4 |  |  |
| 1 | 0 | 400000 | mosquitto | A `8-17` | 16835.936, 8690.598, 13958.644, 18152.948, 17465.082 | 0/5 | 13346.275, 4852.810, 9957.278, 13363.053, 13421.773 | 0.5, 0.3, 0.4, 0.3, 0.2 | 1: missed slots, lateness ratio, steady state; 2: missed slots, lateness ratio, steady state; 3: missed slots, lateness ratio, steady state; 4: missed slots, lateness ratio, steady state; 5: missed slots, lateness ratio, steady state | no |
| 1 | 0 | 400000 | mosquitto | B `1,3,5,7,8-13` | 6522.143, 14059.307, 17465.082, 10485.760, 10133.438 | 0/5 | 3886.023, 10863.247, 13086.228, 6677.332, 6094.324 | 0.3, 0.4, 0.3, 0.3, 0.4 | 1: missed slots, lateness ratio, steady state; 2: missed slots, lateness ratio, steady state; 3: missed slots, lateness ratio, steady state; 4: missed slots, lateness ratio, steady state; 5: missed slots, lateness ratio, steady state |  |
| 4 | 1 | 900000 | uring | A `8-17` | 171.442, 25.674, 149.684, 265.945, 22.053 | 5/5 | 3.090, 2.390, 2.568, 2.462, 1.897 | 2.5, 1.7, 1.7, 1.9, 1.7 |  | no |
| 4 | 1 | 900000 | uring | B `1,3,5,7,8-13` | 47.383, 37.814, 21.152, 25.772, 23.806 | 5/5 | 2.839, 2.116, 1.886, 2.523, 1.756 | 1.6, 1.3, 1.3, 1.4, 1.4 |  |  |
| 4 | 1 | 900000 | tokio | A `8-17` | 104.137, 4292.870, 1303.380, 105.447, 182.714 | 4/5 | 2.652, 2.542, 2.065, 1.548, 2.593 | 2.2, 1.5, 2.0, 1.7, 1.9 | 2: steady state | no |
| 4 | 1 | 900000 | tokio | B `1,3,5,7,8-13` | 68.157, 303.563, 28.869, 32.621, 1104.151 | 5/5 | 2.290, 2.681, 2.084, 2.071, 2.114 | 1.6, 1.3, 1.4, 1.3, 1.3 |  |  |
| 4 | 1 | 1200000 | uring | A `8-17` | 693.109, 2258.633, 152.961, 1344.274, 163.840 | 3/5 | 3.351, 8.667, 2.425, 6.832, 3.158 | 2.1, 1.2, 1.2, 1.7, 1.2 | 1: missed slots; 2: missed slots | no |
| 4 | 1 | 1200000 | uring | B `1,3,5,7,8-13` | 180.748, 62.751, 4120.904, 931.135, 77.791 | 4/5 | 3.568, 2.791, 2340.422, 3.502, 2.650 | 1.5, 1.1, 1.0, 1.1, 1.0 | 3: missed slots, lateness ratio, steady state |  |
| 4 | 1 | 1200000 | tokio | A `8-17` | 4987.027, 7121.928, 5138.022, 1490.026, 5368.709 | 1/5 | 2.441, 4.411, 3.611, 2.986, 3.099 | 1.8, 1.6, 1.8, 1.3, 1.5 | 1: missed slots, steady state; 2: missed slots, steady state; 3: missed slots, steady state; 5: missed slots, steady state | no |
| 4 | 1 | 1200000 | tokio | B `1,3,5,7,8-13` | 6262.096, 8338.276, 6576.669, 745.538, 6241.124 | 0/5 | 3.123, 4.407, 3.467, 2.437, 3.817 | 1.5, 1.2, 1.5, 1.0, 1.3 | 1: missed slots, steady state; 2: missed slots, steady state; 3: missed slots, steady state; 4: steady state; 5: missed slots, steady state |  |
| 4 | 0 | 1300000 | uring | A `8-17` | 67.961, 159.121, 91.161, 318.243, 50.495 | 5/5 | 1.874, 2.078, 2.572, 1.915, 1.975 | 1.3, 1.1, 1.1, 1.2, 1.2 |  | no |
| 4 | 0 | 1300000 | uring | B `1,3,5,7,8-13` | 46.793, 18.891, 36.471, 154.927, 28.787 | 4/5 | 2.286, 1.905, 2.138, 1.629, 2.003 | 1.2, 0.9, 0.9, 0.9, 0.9 | 2: lateness ratio |  |
| 4 | 0 | 1300000 | tokio | A `8-17` | 69.992, 1724.908, 253.100, 84.279, 435.683 | 5/5 | 2.189, 2.427, 2.333, 2.376, 2.781 | 2.6, 1.4, 1.8, 1.3, 1.6 |  | no |
| 4 | 0 | 1300000 | tokio | B `1,3,5,7,8-13` | 3550.478, 33.505, 30.835, 33.063, 42.435 | 4/5 | 3.021, 2.406, 1.637, 2.374, 2.398 | 1.6, 0.9, 0.8, 1.0, 1.0 | 1: missed slots, steady state |  |
| 4 | 0 | 1500000 | uring | A `8-17` | 690.487, 1657.799, 291.242, 141.820, 33.325 | 3/5 | 2.531, 538.968, 2.687, 2.578, 2.329 | 1.5, 0.8, 1.3, 1.0, 1.0 | 2: missed slots, lateness ratio, steady state; 3: missed slots, steady state | no |
| 4 | 0 | 1500000 | uring | B `1,3,5,7,8-13` | 51.380, 34.111, 168.952, 3269.460, 1051.197 | 3/5 | 2.703, 2.351, 2.714, 2079.326, 120.979 | 1.2, 0.9, 1.0, 0.8, 0.7 | 4: missed slots, lateness ratio, steady state; 5: lateness ratio |  |
| 4 | 0 | 1500000 | tokio | A `8-17` | 4890.558, 387.973, 283.640, 1484.784, 3152.019 | 3/5 | 1910.505, 2.902, 2.165, 2.474, 4.588 | 1.7, 1.5, 1.1, 1.3, 1.5 | 1: missed slots, lateness ratio, steady state; 5: missed slots, steady state | no |
| 4 | 0 | 1500000 | tokio | B `1,3,5,7,8-13` | 361.234, 193.462, 75.432, 64.848, 78.643 | 5/5 | 3.273, 2.968, 2.296, 2.421, 2.648 | 1.2, 0.8, 0.8, 0.8, 0.8 |  |  |

### Experiment 1 verdict: robust

Q6 rule: a configuration differs when every repetition p99 of one arm exceeds every repetition p99 of
the other, or when the arms' valid counts differ by 4 or more. Sensitive is 2 or more configurations
that differ in the same direction. Robust is no configuration that differs. Inconclusive is any other
result.

- 0 of the 20 configurations differ. The verdict is **robust**.
- No configuration separates the arms on p99. The repetition p99 ranges of the two arms overlap in
  every configuration.
- The largest valid-count gap is 3: mosquitto w1 q1 75 000 (A 2/5, B 5/5; every A failure is the
  steady-state row). The next largest gap is 2: tokio twin w4 q0 1 500 000 (A 3/5, B 5/5).

#### Scope of experiment 1

The verdict covers only the two pre-registered 10-CPU arms. It says nothing about generator CPU
count, about physical core sharing on the host, or about effects smaller than the 5-repetition
separation test can detect. Under no effect, the p99 condition fires with a chance of about 0.8 %
per configuration (Q6).

#### Observations (not applied)

These observations come from the data. No rule uses them.

- At 4 workers, host CPU pressure is higher in arm A than in arm B in 39 of the 40 (repetition,
  configuration) pairs (for example w4 q1 900 000 uring: A 1.7-2.5 %, B 1.3-1.6 %). At 1 worker it is
  higher in arm A in 20 of 60 pairs. Pressure is evidence only.
- At w1 q0 400 000, the generator-lateness p99 is higher in arm B in 4 of 5 uring pairs and in all 5
  tokio twin pairs, but the arm ranges overlap. Lateness is not a Q6 statistic.
- The tokio twin at w4 q1 1 200 000 (a declared rate) was valid in 1 of 10 runs across both arms,
  with missed slots and steady-state failures. uring at that rate was valid in 7 of 10.

## Experiment 2: per-configuration results

Placement `8-17`, server CPUs `0,2,4,6`, 4 workers, QoS 1. All 40 runs are invalid.

| Workers | QoS | Rate (msgs/s) | Server | Arm | Repetition p99 (ms), rep 1-5 | Valid | Lateness p99 (ms), rep 1-5 | Pressure (%), rep 1-5 |
|---|---|---|---|---|---|---|---|---|
| 4 | 1 | 1300000 | uring | A `8-17` | 3399.483, 2699.035, 5104.468, 2126.512, 3460.301 | 0/5 | 432.538, 214.696, 3932.160, 2.879, 1793.065 | 1.2, 1.7, 1.9, 1.3, 1.2 |
| 4 | 1 | 1300000 | tokio | A `8-17` | 6509.560, 5351.932, 7042.236, 5674.893, 6119.490 | 0/5 | 4.016, 6.926, 3.498, 2.640, 3.199 | 1.7, 1.9, 1.7, 1.2, 1.4 |
| 4 | 1 | 1500000 | uring | A `8-17` | 9026.142, 4563.403, 20266.877, 7046.431, 3061.842 | 0/5 | 7902.069, 3563.061, 18656.264, 5448.401, 1799.356 | 1.2, 1.0, 1.5, 1.2, 1.2 |
| 4 | 1 | 1500000 | tokio | A `8-17` | 6652.166, 5989.466, 5242.880, 6153.044, 6023.021 | 0/5 | 12.018, 3594.519, 3.736, 4.551, 322.961 | 2.2, 1.7, 1.2, 1.3, 1.2 |
| 4 | 1 | 1700000 | uring | A `8-17` | 9135.194, 6094.324, 17129.538, 13178.503, 4550.820 | 0/5 | 8204.059, 4680.843, 15753.806, 11987.321, 3615.490 | 0.8, 1.3, 1.2, 0.9, 1.0 |
| 4 | 1 | 1700000 | tokio | A `8-17` | 6224.347, 6891.241, 4777.312, 5364.515, 8501.854 | 0/5 | 1868.562, 920.650, 4.256, 1150.288, 5368.709 | 2.1, 1.2, 1.3, 1.3, 1.5 |
| 4 | 1 | 2000000 | uring | A `8-17` | 14352.908, 14143.193, 15762.194, 13010.731, 4454.351 | 0/5 | 13665.042, 12977.177, 14914.945, 11928.601, 3445.621 | 0.9, 0.9, 0.7, 1.9, 0.6 |
| 4 | 1 | 2000000 | tokio | A `8-17` | 5007.999, 11794.383, 9890.169, 8707.375, 7193.231 | 0/5 | 1356.857, 8527.020, 7092.568, 6048.186, 3244.294 | 1.4, 1.4, 1.1, 1.5, 1.3 |

## 4-worker runs with `worker_loads`

Every 4-worker run of both experiments (80 from experiment 1, 40 from experiment 2). The
`worker_loads` cell lists `connections/delivered/p99 ms` per worker, busiest first. Each run has
100 connections. No worker has zero connections, so no worker is left out of the
delivered-per-connection minimum. The last column is the Q7 rule result for invalid runs.

| Exp | Arm | QoS | Rate (msgs/s) | Server | Rep | Valid | p99 (ms) | Lateness p99 (ms) | Pressure (%) | Failed rows | worker_loads | Q7 rule (invalid runs) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | A | 1 | 900000 | uring | 1 | yes | 171.442 | 3.090 | 2.5 |  | 32/20157120/243.532 · 27/17007570/37.585 · 21/13228110/28.328 · 20/12598200/24.035 |  |
| 1 | A | 1 | 900000 | uring | 2 | yes | 25.674 | 2.390 | 1.7 |  | 30/18897300/34.734 · 24/15117840/24.904 · 24/15117840/26.100 · 22/13858020/19.497 |  |
| 1 | A | 1 | 900000 | uring | 3 | yes | 149.684 | 2.568 | 1.7 |  | 28/17637480/21.742 · 26/16377660/320.340 · 24/15117840/41.583 · 22/13858020/163.054 |  |
| 1 | A | 1 | 900000 | uring | 4 | yes | 265.945 | 2.462 | 1.9 |  | 28/17637480/45.220 · 25/15747750/445.907 · 24/15117840/16.425 · 23/14487930/250.216 |  |
| 1 | A | 1 | 900000 | uring | 5 | yes | 22.053 | 1.897 | 1.7 |  | 33/20787030/10.002 · 29/18267390/30.114 · 21/13228110/12.665 · 17/10708470/16.703 |  |
| 1 | B | 1 | 900000 | uring | 1 | yes | 47.383 | 2.839 | 1.6 |  | 27/17007570/67.502 · 26/16377660/46.334 · 25/15747750/41.648 · 22/13858020/30.999 |  |
| 1 | B | 1 | 900000 | uring | 2 | yes | 37.814 | 2.116 | 1.3 |  | 30/18897300/69.206 · 25/15747750/34.767 · 24/15117840/9.249 · 21/13228110/21.905 |  |
| 1 | B | 1 | 900000 | uring | 3 | yes | 21.152 | 1.886 | 1.3 |  | 30/18897300/21.873 · 25/15747750/6.803 · 25/15747750/26.591 · 20/12598200/19.775 |  |
| 1 | B | 1 | 900000 | uring | 4 | yes | 25.772 | 2.523 | 1.4 |  | 31/19527210/25.985 · 27/17007570/30.605 · 24/15117840/23.151 · 18/11338380/16.114 |  |
| 1 | B | 1 | 900000 | uring | 5 | yes | 23.806 | 1.756 | 1.4 |  | 32/20157120/10.322 · 31/19527210/35.979 · 19/11968290/11.878 · 18/11338380/11.600 |  |
| 1 | A | 1 | 900000 | tokio | 1 | yes | 104.137 | 2.652 | 2.2 |  | 26/16377660/39.092 · 25/15747750/215.745 · 25/15747750/59.539 · 24/15117840/142.344 |  |
| 1 | A | 1 | 900000 | tokio | 2 | no | 4292.870 | 2.542 | 1.5 | steady state | 33/20787030/5087.691 · 27/17007570/28.082 · 24/15117840/11.805 · 16/10078560/14.737 | holds: busiest (33, 1 tied) holds both |
| 1 | A | 1 | 900000 | tokio | 3 | yes | 1303.380 | 2.065 | 2.0 |  | 32/20157120/1724.908 · 25/15747750/932.708 · 22/13858020/93.585 · 21/13228110/30.065 |  |
| 1 | A | 1 | 900000 | tokio | 4 | yes | 105.447 | 1.548 | 1.7 |  | 31/19527210/174.981 · 26/16377660/92.602 · 25/15747750/117.768 · 18/11338380/17.400 |  |
| 1 | A | 1 | 900000 | tokio | 5 | yes | 182.714 | 2.593 | 1.9 |  | 28/17637480/33.014 · 26/16377660/318.505 · 26/16377660/365.167 · 20/12598200/18.366 |  |
| 1 | B | 1 | 900000 | tokio | 1 | yes | 68.157 | 2.290 | 1.6 |  | 28/17637480/136.053 · 27/17007570/47.284 · 24/15117840/34.144 · 21/13228110/28.525 |  |
| 1 | B | 1 | 900000 | tokio | 2 | yes | 303.563 | 2.681 | 1.3 |  | 30/18897300/421.528 · 25/15747750/44.663 · 23/14487930/38.273 · 22/13858020/33.473 |  |
| 1 | B | 1 | 900000 | tokio | 3 | yes | 28.869 | 2.084 | 1.4 |  | 41/25826310/37.028 · 23/14487930/28.426 · 21/13228110/22.168 · 15/9448650/17.596 |  |
| 1 | B | 1 | 900000 | tokio | 4 | yes | 32.621 | 2.071 | 1.3 |  | 28/17637480/53.182 · 26/16377660/22.741 · 24/15117840/28.377 · 22/13858020/22.184 |  |
| 1 | B | 1 | 900000 | tokio | 5 | yes | 1104.151 | 2.114 | 1.3 |  | 35/22046850/1437.598 · 23/14487930/26.706 · 22/13858020/23.233 · 20/12598200/15.081 |  |
| 1 | A | 1 | 1200000 | uring | 1 | no | 693.109 | 3.351 | 2.1 | missed slots | 28/23516640/809.501 · 25/20997000/577.765 · 25/20997000/580.911 · 22/18468572/478.675 | fails: busiest (28, 1 tied): not lowest delivered/connection |
| 1 | A | 1 | 1200000 | uring | 2 | no | 2258.633 | 8.667 | 1.2 | missed slots | 29/24339573/3758.096 · 27/22676760/60.817 · 24/20157120/25.919 · 20/16797600/32.276 | holds: busiest (29, 1 tied) holds both |
| 1 | A | 1 | 1200000 | uring | 3 | yes | 152.961 | 2.425 | 1.2 |  | 26/21836880/12.829 · 26/21836880/202.113 · 24/20157120/214.303 · 24/20157120/28.541 |  |
| 1 | A | 1 | 1200000 | uring | 4 | yes | 1344.274 | 6.832 | 1.7 |  | 33/27716040/1103.102 · 27/22676760/618.660 · 23/19317240/1570.767 · 17/14277960/603.455 |  |
| 1 | A | 1 | 1200000 | uring | 5 | yes | 163.840 | 3.158 | 1.2 |  | 28/23516640/697.827 · 26/21836880/73.794 · 23/19317240/47.809 · 23/19317240/15.557 |  |
| 1 | B | 1 | 1200000 | uring | 1 | yes | 180.748 | 3.568 | 1.5 |  | 33/27716040/36.438 · 25/20997000/638.583 · 23/19317240/185.205 · 19/15957720/46.072 |  |
| 1 | B | 1 | 1200000 | uring | 2 | yes | 62.751 | 2.791 | 1.1 |  | 27/22676760/19.513 · 25/20997000/56.918 · 25/20997000/173.408 · 23/19317240/54.985 |  |
| 1 | B | 1 | 1200000 | uring | 3 | no | 4120.904 | 2340.422 | 1.0 | missed slots, lateness ratio, steady state | 35/28235375/5419.041 · 25/20997000/194.773 · 21/17637480/33.538 · 19/15957720/16.794 | holds: busiest (35, 1 tied) holds both |
| 1 | B | 1 | 1200000 | uring | 4 | yes | 931.135 | 3.502 | 1.1 |  | 30/25196400/3720.348 · 25/20997000/42.336 · 24/20157120/69.272 · 21/17637480/27.689 |  |
| 1 | B | 1 | 1200000 | uring | 5 | yes | 77.791 | 2.650 | 1.0 |  | 32/26876160/24.723 · 24/20157120/109.445 · 23/19317240/132.645 · 21/17637480/34.013 |  |
| 1 | A | 1 | 1200000 | tokio | 1 | no | 4987.027 | 2.441 | 1.8 | missed slots, steady state | 28/23362509/5423.235 · 27/22676760/4278.190 · 25/20997000/718.275 · 20/16797600/128.516 | holds: busiest (28, 1 tied) holds both |
| 1 | A | 1 | 1200000 | tokio | 2 | no | 7121.928 | 4.411 | 1.6 | missed slots, steady state | 29/20126810/7528.776 · 25/20997000/676.332 · 23/19317240/847.249 · 23/19317240/1279.263 | holds: busiest (29, 1 tied) holds both |
| 1 | A | 1 | 1200000 | tokio | 3 | no | 5138.022 | 3.611 | 1.8 | missed slots, steady state | 28/23131662/5746.196 · 25/20997000/2770.338 · 24/20157120/65.929 · 23/19317240/314.835 | holds: busiest (28, 1 tied) holds both |
| 1 | A | 1 | 1200000 | tokio | 4 | yes | 1490.026 | 2.986 | 1.3 |  | 27/22676760/282.329 · 27/22676760/1692.402 · 25/20997000/278.397 · 21/17637480/56.328 |  |
| 1 | A | 1 | 1200000 | tokio | 5 | no | 5368.709 | 3.099 | 1.5 | missed slots, steady state | 38/31446412/5586.813 · 27/22676760/2269.118 · 23/19317240/53.871 · 12/10078560/19.333 | holds: busiest (38, 1 tied) holds both |
| 1 | B | 1 | 1200000 | tokio | 1 | no | 6262.096 | 3.123 | 1.5 | missed slots, steady state | 32/26876160/2062.549 · 28/21361158/6652.166 · 21/17637480/76.677 · 19/15957720/52.167 | fails: busiest (32, 1 tied): not lowest delivered/connection, not highest p99 |
| 1 | B | 1 | 1200000 | tokio | 2 | no | 8338.276 | 4.407 | 1.2 | missed slots, steady state | 35/21371381/8925.479 · 26/21836880/418.644 · 21/17637480/21.692 · 18/15117840/41.255 | holds: busiest (35, 1 tied) holds both |
| 1 | B | 1 | 1200000 | tokio | 3 | no | 6576.669 | 3.467 | 1.5 | missed slots, steady state | 33/24755920/6866.076 · 28/23516640/2397.045 · 24/20157120/152.830 · 15/12598200/37.487 | holds: busiest (33, 1 tied) holds both |
| 1 | B | 1 | 1200000 | tokio | 4 | no | 745.538 | 2.437 | 1.0 | steady state | 28/23516640/917.504 · 25/20997000/68.551 · 24/20157120/217.973 · 23/19317240/28.148 | holds: busiest (28, 1 tied) holds both |
| 1 | B | 1 | 1200000 | tokio | 5 | no | 6241.124 | 3.817 | 1.3 | missed slots, steady state | 29/23624119/6035.603 · 29/21906835/6752.829 · 22/18477360/189.399 · 20/16797600/39.485 | holds: busiest (29, 2 tied) holds both |
| 1 | A | 0 | 1300000 | uring | 1 | yes | 67.961 | 1.874 | 1.3 |  | 29/26386230/31.703 · 26/23656620/229.769 · 25/22746750/26.509 · 20/18197400/35.619 |  |
| 1 | A | 0 | 1300000 | uring | 2 | yes | 159.121 | 2.078 | 1.1 |  | 27/24566490/337.904 · 25/22746750/22.594 · 25/22746750/25.608 · 23/20927010/233.570 |  |
| 1 | A | 0 | 1300000 | uring | 3 | yes | 91.161 | 2.572 | 1.1 |  | 29/26386230/170.394 · 27/24566490/22.528 · 26/23656620/89.391 · 18/16377660/19.497 |  |
| 1 | A | 0 | 1300000 | uring | 4 | yes | 318.243 | 1.915 | 1.2 |  | 33/30025710/511.705 · 24/21836880/105.972 · 22/20017140/18.956 · 21/19107270/20.070 |  |
| 1 | A | 0 | 1300000 | uring | 5 | yes | 50.495 | 1.975 | 1.2 |  | 31/28205970/9.994 · 25/22746750/116.982 · 24/21836880/122.159 · 20/18197400/23.396 |  |
| 1 | B | 0 | 1300000 | uring | 1 | yes | 46.793 | 2.286 | 1.2 |  | 30/27296100/70.779 · 28/25476360/12.304 · 21/19107270/25.133 · 21/19107270/23.577 |  |
| 1 | B | 0 | 1300000 | uring | 2 | no | 18.891 | 1.905 | 0.9 | lateness ratio | 29/26386230/8.397 · 28/25476360/25.526 · 25/22746750/19.759 · 18/16377660/11.772 | fails: busiest (29, 1 tied): not highest p99 |
| 1 | B | 0 | 1300000 | uring | 3 | yes | 36.471 | 2.138 | 0.9 |  | 30/27296100/68.485 · 28/25476360/30.032 · 25/22746750/18.268 · 17/15467790/21.398 |  |
| 1 | B | 0 | 1300000 | uring | 4 | yes | 154.927 | 1.629 | 0.9 |  | 31/28205970/182.452 · 25/22746750/32.440 · 23/20927010/18.547 · 21/19107270/25.969 |  |
| 1 | B | 0 | 1300000 | uring | 5 | yes | 28.787 | 2.003 | 0.9 |  | 33/30025710/42.074 · 27/24566490/28.017 · 20/18197400/11.239 · 20/18197400/16.253 |  |
| 1 | A | 0 | 1300000 | tokio | 1 | yes | 69.992 | 2.189 | 2.6 |  | 28/25476360/75.301 · 28/25476360/133.825 · 24/21836880/43.516 · 20/18197400/22.331 |  |
| 1 | A | 0 | 1300000 | tokio | 2 | yes | 1724.908 | 2.427 | 1.4 |  | 34/30935580/2090.861 · 26/23656620/306.708 · 22/20017140/168.821 · 18/16377660/23.282 |  |
| 1 | A | 0 | 1300000 | tokio | 3 | yes | 253.100 | 2.333 | 1.8 |  | 27/24566490/88.670 · 26/23656620/460.849 · 24/21836880/244.842 · 23/20927010/56.590 |  |
| 1 | A | 0 | 1300000 | tokio | 4 | yes | 84.279 | 2.376 | 1.3 |  | 28/25476360/218.759 · 26/23656620/32.621 · 23/20927010/54.297 · 23/20927010/99.090 |  |
| 1 | A | 0 | 1300000 | tokio | 5 | yes | 435.683 | 2.781 | 1.6 |  | 35/31845450/555.745 · 23/20927010/67.437 · 22/20017140/44.499 · 20/18197400/32.588 |  |
| 1 | B | 0 | 1300000 | tokio | 1 | no | 3550.478 | 3.021 | 1.6 | missed slots, steady state | 36/32753121/3892.314 · 28/25476360/27.787 · 22/20017140/42.893 · 14/12738180/21.365 | holds: busiest (36, 1 tied) holds both |
| 1 | B | 0 | 1300000 | tokio | 2 | yes | 33.505 | 2.406 | 0.9 |  | 28/25476360/44.073 · 25/22746750/31.670 · 24/21836880/30.507 · 23/20927010/26.477 |  |
| 1 | B | 0 | 1300000 | tokio | 3 | yes | 30.835 | 1.637 | 0.8 |  | 29/26386230/46.563 · 26/23656620/20.349 · 24/21836880/25.084 · 21/19107270/19.317 |  |
| 1 | B | 0 | 1300000 | tokio | 4 | yes | 33.063 | 2.374 | 1.0 |  | 28/25476360/29.966 · 27/24566490/44.106 · 23/20927010/30.196 · 22/20017140/19.890 |  |
| 1 | B | 0 | 1300000 | tokio | 5 | yes | 42.435 | 2.398 | 1.0 |  | 27/24566490/68.354 · 25/22746750/28.049 · 24/21836880/35.127 · 24/21836880/34.013 |  |
| 1 | A | 0 | 1500000 | uring | 1 | yes | 690.487 | 2.531 | 1.5 |  | 30/31495500/364.642 · 25/26246250/33.620 · 24/25196400/945.291 · 21/22046850/494.404 |  |
| 1 | A | 0 | 1500000 | uring | 2 | no | 1657.799 | 538.968 | 0.8 | missed slots, lateness ratio, steady state | 34/35649890/1941.963 · 25/26246250/54.624 · 22/23096700/13.844 · 19/19947150/16.474 | holds: busiest (34, 1 tied) holds both |
| 1 | A | 0 | 1500000 | uring | 3 | no | 291.242 | 2.687 | 1.3 | missed slots, steady state | 29/30438876/357.564 · 27/28345791/371.720 · 24/25196400/28.066 · 20/20997000/147.325 | fails: busiest (29, 1 tied): not highest p99 |
| 1 | A | 0 | 1500000 | uring | 4 | yes | 141.820 | 2.578 | 1.0 |  | 27/28345950/48.726 · 25/26246250/262.275 · 25/26246250/116.261 · 23/24146550/74.777 |  |
| 1 | A | 0 | 1500000 | uring | 5 | yes | 33.325 | 2.329 | 1.0 |  | 30/31495500/37.323 · 24/25196400/39.780 · 23/24146550/20.939 · 23/24146550/28.099 |  |
| 1 | B | 0 | 1500000 | uring | 1 | yes | 51.380 | 2.703 | 1.2 |  | 28/29395800/66.585 · 27/28345950/63.963 · 24/25196400/12.321 · 21/22046850/32.440 |  |
| 1 | B | 0 | 1500000 | uring | 2 | yes | 34.111 | 2.351 | 0.9 |  | 33/34645050/16.925 · 27/28345950/48.169 · 23/24146550/32.686 · 17/17847450/20.742 |  |
| 1 | B | 0 | 1500000 | uring | 3 | yes | 168.952 | 2.714 | 1.0 |  | 34/35694900/629.670 · 27/28345950/80.544 · 25/26246250/34.800 · 14/14697900/14.197 |  |
| 1 | B | 0 | 1500000 | uring | 4 | no | 3269.460 | 2079.326 | 0.8 | missed slots, lateness ratio, steady state | 35/36446341/3468.689 · 23/24146550/40.337 · 21/22046850/20.922 · 21/22046850/25.248 | holds: busiest (35, 1 tied) holds both |
| 1 | B | 0 | 1500000 | uring | 5 | no | 1051.197 | 120.979 | 0.7 | lateness ratio | 29/30445650/1298.137 · 26/27296100/46.170 · 26/27296100/33.243 · 19/19947150/12.517 | holds: busiest (29, 1 tied) holds both |
| 1 | A | 0 | 1500000 | tokio | 1 | no | 4890.558 | 1910.505 | 1.7 | missed slots, lateness ratio, steady state | 38/39534350/5666.505 · 22/23096700/460.063 · 21/22046850/41.779 · 19/19947150/164.495 | holds: busiest (38, 1 tied) holds both |
| 1 | A | 0 | 1500000 | tokio | 2 | yes | 387.973 | 2.902 | 1.5 |  | 29/30445650/374.079 · 27/28345950/536.871 · 25/26246250/378.012 · 19/19947150/175.243 |  |
| 1 | A | 0 | 1500000 | tokio | 3 | yes | 283.640 | 2.165 | 1.1 |  | 34/35694900/500.433 · 24/25196400/63.046 · 21/22046850/35.193 · 21/22046850/39.584 |  |
| 1 | A | 0 | 1500000 | tokio | 4 | yes | 1484.784 | 2.474 | 1.3 |  | 32/33595200/1855.980 · 24/25196400/391.119 · 23/24146550/137.495 · 21/22046850/109.117 |  |
| 1 | A | 0 | 1500000 | tokio | 5 | no | 3152.019 | 4.588 | 1.5 | missed slots, steady state | 37/38844450/3529.507 · 24/25186251/533.463 · 23/24146550/567.280 · 16/16797600/34.439 | fails: busiest (37, 1 tied): not lowest delivered/connection |
| 1 | B | 0 | 1500000 | tokio | 1 | yes | 361.234 | 3.273 | 1.2 |  | 28/29395800/547.881 · 27/28345950/129.958 · 24/25196400/45.810 · 21/22046850/45.646 |  |
| 1 | B | 0 | 1500000 | tokio | 2 | yes | 193.462 | 2.968 | 0.8 |  | 30/31495500/280.756 · 27/28345950/73.466 · 23/24146550/43.385 · 20/20997000/32.539 |  |
| 1 | B | 0 | 1500000 | tokio | 3 | yes | 75.432 | 2.296 | 0.8 |  | 28/29395800/133.431 · 27/28345950/32.031 · 23/24146550/29.671 · 22/23096700/41.091 |  |
| 1 | B | 0 | 1500000 | tokio | 4 | yes | 64.848 | 2.421 | 0.8 |  | 27/28345950/78.905 · 27/28345950/19.907 · 26/27296100/90.309 · 20/20997000/42.762 |  |
| 1 | B | 0 | 1500000 | tokio | 5 | yes | 78.643 | 2.648 | 0.8 |  | 28/29395800/123.798 · 26/27296100/81.854 · 25/26246250/43.811 · 21/22046850/27.476 |  |
| 2 | A | 1 | 1300000 | uring | 1 | no | 3399.483 | 432.538 | 1.2 | missed slots, lateness ratio, steady state | 28/25089568/7964.983 · 27/24499332/3795.845 · 25/22746750/105.382 · 20/18197400/26.591 | holds: busiest (28, 1 tied) holds both |
| 2 | A | 1 | 1300000 | uring | 2 | no | 2699.035 | 214.696 | 1.7 | missed slots, steady state | 30/27180241/567.804 · 29/25549561/3938.451 · 23/20927010/800.063 · 18/16377660/368.574 | fails: busiest (30, 1 tied): not lowest delivered/connection, not highest p99 |
| 2 | A | 1 | 1300000 | uring | 3 | no | 5104.468 | 3932.160 | 1.9 | missed slots, lateness ratio, steady state | 33/28908446/5557.453 · 25/22746750/112.263 · 21/19107270/55.476 · 21/19107270/392.430 | holds: busiest (33, 1 tied) holds both |
| 2 | A | 1 | 1300000 | uring | 4 | no | 2126.512 | 2.879 | 1.3 | missed slots, steady state | 30/27296100/20.316 · 27/23749790/2839.544 · 26/23656620/1631.584 · 17/15467790/65.569 | fails: busiest (30, 1 tied): not lowest delivered/connection, not highest p99 |
| 2 | A | 1 | 1300000 | uring | 5 | no | 3460.301 | 1793.065 | 1.2 | missed slots, lateness ratio, steady state | 33/29012297/4259.316 · 27/24566490/60.621 · 21/19107270/109.117 · 19/17287530/61.735 | holds: busiest (33, 1 tied) holds both |
| 2 | A | 1 | 1300000 | tokio | 1 | no | 6509.560 | 4.016 | 1.7 | missed slots, steady state | 32/24108663/7285.506 · 30/27296100/155.451 · 21/19107270/425.198 · 17/15467790/43.581 | holds: busiest (32, 1 tied) holds both |
| 2 | A | 1 | 1300000 | tokio | 2 | no | 5351.932 | 6.926 | 1.9 | missed slots, steady state | 31/28165242/4563.403 · 27/24160765/5226.103 · 26/22837939/6140.461 · 16/14557920/48.202 | fails: busiest (31, 1 tied): not lowest delivered/connection, not highest p99 |
| 2 | A | 1 | 1300000 | tokio | 3 | no | 7042.236 | 3.498 | 1.7 | missed slots, steady state | 34/25410143/7348.421 · 28/23341682/5884.609 · 23/20927010/1011.876 · 15/13648050/197.526 | holds: busiest (34, 1 tied) holds both |
| 2 | A | 1 | 1300000 | tokio | 4 | no | 5674.893 | 2.640 | 1.2 | missed slots, steady state | 29/26386230/32.309 · 27/22209868/5955.912 · 23/20927010/104.399 · 21/19107270/45.613 | fails: busiest (29, 1 tied): not lowest delivered/connection, not highest p99 |
| 2 | A | 1 | 1300000 | tokio | 5 | no | 6119.490 | 3.199 | 1.4 | missed slots, steady state | 33/26604577/6383.731 · 25/22746750/1471.152 · 23/20927010/239.993 · 19/17287530/185.729 | holds: busiest (33, 1 tied) holds both |
| 2 | A | 1 | 1500000 | uring | 1 | no | 9026.142 | 7902.069 | 1.2 | missed slots, lateness ratio, steady state | 30/28016877/10007.609 · 25/25296224/2577.400 · 24/25196400/315.884 · 21/22046850/135.135 | holds: busiest (30, 1 tied) holds both |
| 2 | A | 1 | 1500000 | uring | 2 | no | 4563.403 | 3563.061 | 1.0 | missed slots, lateness ratio, steady state | 30/31020707/38453.379 · 25/26067362/1391.460 · 24/25196400/632.816 · 21/22046850/318.767 | holds: busiest (30, 1 tied) holds both |
| 2 | A | 1 | 1500000 | uring | 3 | no | 20266.877 | 18656.264 | 1.5 | missed slots, lateness ratio, steady state | 37/27434506/20501.758 · 26/27296100/53.740 · 20/20997000/79.626 · 17/17847450/55.411 | holds: busiest (37, 1 tied) holds both |
| 2 | A | 1 | 1500000 | uring | 4 | no | 7046.431 | 5448.401 | 1.2 | missed slots, lateness ratio, steady state | 29/27962634/6585.057 · 27/28327823/32967.229 · 22/23096700/223.347 · 22/23096700/93.913 | fails: busiest (29, 1 tied): not highest p99 |
| 2 | A | 1 | 1500000 | uring | 5 | no | 3061.842 | 1799.356 | 1.2 | missed slots, lateness ratio, steady state | 29/30262875/1301.283 · 26/26709548/43822.088 · 26/26373604/2980.053 · 19/19947150/48.497 | fails: busiest (29, 1 tied): not lowest delivered/connection, not highest p99 |
| 2 | A | 1 | 1500000 | tokio | 1 | no | 6652.166 | 12.018 | 2.2 | missed slots, steady state | 31/28578972/7348.421 · 26/23456011/5855.248 · 24/24506740/4995.416 · 19/19947150/402.391 | fails: busiest (31, 1 tied): not lowest delivered/connection |
| 2 | A | 1 | 1500000 | tokio | 2 | no | 5989.466 | 3594.519 | 1.7 | missed slots, lateness ratio, steady state | 27/24835641/6589.252 · 26/23141416/5658.116 · 25/25929481/5351.932 · 22/23096700/143.131 | fails: busiest (27, 1 tied): not lowest delivered/connection |
| 2 | A | 1 | 1500000 | tokio | 3 | no | 5242.880 | 3.736 | 1.2 | missed slots, steady state | 26/24657931/5129.634 · 26/23426682/5486.150 · 25/26246250/37.356 · 23/23668534/4538.237 | holds: busiest (26, 2 tied) holds both |
| 2 | A | 1 | 1500000 | tokio | 4 | no | 6153.044 | 4.551 | 1.3 | missed slots, steady state | 30/24135143/6446.645 · 27/28345950/54.886 · 23/24146550/413.925 · 20/20997000/139.985 | holds: busiest (30, 1 tied) holds both |
| 2 | A | 1 | 1500000 | tokio | 5 | no | 6023.021 | 322.961 | 1.2 | missed slots, steady state | 28/27668642/6589.252 · 26/23865249/6023.021 · 25/22630567/5620.367 · 21/21616268/4609.540 | fails: busiest (28, 1 tied): not lowest delivered/connection |
| 2 | A | 1 | 1700000 | uring | 1 | no | 9135.194 | 8204.059 | 0.8 | missed slots, lateness ratio, steady state | 30/30425067/10485.760 · 27/32125410/59.998 · 25/28654190/3133.145 · 18/21416940/38.502 | holds: busiest (30, 1 tied) holds both |
| 2 | A | 1 | 1700000 | uring | 2 | no | 6094.324 | 4680.843 | 1.3 | missed slots, lateness ratio, steady state | 28/30717663/6920.602 · 27/31299295/36238.787 · 23/27366090/1294.991 · 22/26176260/493.355 | fails: busiest (28, 1 tied): not highest p99 |
| 2 | A | 1 | 1700000 | uring | 3 | no | 17129.538 | 15753.806 | 1.2 | missed slots, lateness ratio, steady state | 35/31102706/17314.087 · 23/27366090/262.406 · 23/27073309/14596.178 · 19/22606770/225.575 | holds: busiest (35, 1 tied) holds both |
| 2 | A | 1 | 1700000 | uring | 4 | no | 13178.503 | 11987.321 | 0.9 | missed slots, lateness ratio, steady state | 33/31427622/13631.488 · 29/34505070/53.936 · 24/28552942/4404.019 · 14/16657620/53.019 | holds: busiest (33, 1 tied) holds both |
| 2 | A | 1 | 1700000 | uring | 5 | no | 4550.820 | 3615.490 | 1.0 | missed slots, lateness ratio, steady state | 29/31645325/6203.376 · 26/29956546/2548.040 · 24/27471449/3586.130 · 21/24986430/42.861 | holds: busiest (29, 1 tied) holds both |
| 2 | A | 1 | 1700000 | tokio | 1 | no | 6224.347 | 1868.562 | 2.1 | missed slots, lateness ratio, steady state | 29/24923684/6882.853 · 26/25210604/5175.771 · 23/25329865/5448.401 · 22/25363076/4483.711 | holds: busiest (29, 1 tied) holds both |
| 2 | A | 1 | 1700000 | tokio | 2 | no | 6891.241 | 920.650 | 1.2 | missed slots, lateness ratio, steady state | 30/22867814/7478.444 · 27/24994379/5968.495 · 24/28555920/2212.495 · 19/22606770/2443.182 | holds: busiest (30, 1 tied) holds both |
| 2 | A | 1 | 1700000 | tokio | 3 | no | 4777.312 | 4.256 | 1.3 | missed slots, steady state | 29/33213335/4139.778 · 26/25904375/4978.639 · 23/24422236/4596.957 · 22/23852976/4500.488 | fails: busiest (29, 1 tied): not lowest delivered/connection, not highest p99 |
| 2 | A | 1 | 1700000 | tokio | 4 | no | 5364.515 | 1150.288 | 1.3 | missed slots, lateness ratio, steady state | 27/25892299/5490.344 · 26/24620929/5595.202 · 24/25266653/4764.729 · 23/27366090/1008.730 | fails: busiest (27, 1 tied): not lowest delivered/connection, not highest p99 |
| 2 | A | 1 | 1700000 | tokio | 5 | no | 8501.854 | 5368.709 | 1.5 | missed slots, lateness ratio, steady state | 30/24201527/9915.335 · 28/33315240/1110.442 · 21/24986430/2466.251 · 21/24626828/4001.366 | holds: busiest (30, 1 tied) holds both |
| 2 | A | 1 | 2000000 | uring | 1 | no | 14352.908 | 13665.042 | 0.9 | missed slots, lateness ratio, steady state | 30/32893905/15518.925 · 26/34600170/4949.279 · 22/30795600/3359.638 · 22/30794559/22481.469 | fails: busiest (30, 1 tied): not highest p99 |
| 2 | A | 1 | 2000000 | uring | 2 | no | 14143.193 | 12977.177 | 0.9 | missed slots, lateness ratio, steady state | 28/30618831/15074.329 · 26/36394800/206.045 · 26/32823625/6425.674 · 20/27996000/1506.804 | holds: busiest (28, 1 tied) holds both |
| 2 | A | 1 | 2000000 | uring | 3 | no | 15762.194 | 14914.945 | 0.7 | missed slots, lateness ratio, steady state | 29/39765103/18941.477 · 29/30975290/15753.806 · 21/29395800/990.380 · 21/28071266/2254.438 | fails: busiest (29, 2 tied): no single tied worker holds both |
| 2 | A | 1 | 2000000 | uring | 4 | no | 13010.731 | 11928.601 | 1.9 | missed slots, lateness ratio, steady state | 27/30106935/13916.701 · 26/35283979/3810.525 · 25/34007567/8380.219 · 22/30717670/1557.135 | holds: busiest (27, 1 tied) holds both |
| 2 | A | 1 | 2000000 | uring | 5 | no | 4454.351 | 3445.621 | 0.6 | missed slots, lateness ratio, steady state | 31/41824717/4192.207 · 25/32376952/5641.339 · 23/30860969/3508.535 · 21/29395800/993.526 | fails: busiest (31, 1 tied): not lowest delivered/connection, not highest p99 |
| 2 | A | 1 | 2000000 | tokio | 1 | no | 5007.999 | 1356.857 | 1.4 | missed slots, lateness ratio, steady state | 29/34532329/4076.863 · 27/26894014/5247.074 · 23/25494236/4475.322 · 21/26376485/4246.733 | fails: busiest (29, 1 tied): not lowest delivered/connection, not highest p99 |
| 2 | A | 1 | 2000000 | tokio | 2 | no | 11794.383 | 8527.020 | 1.4 | missed slots, lateness ratio, steady state | 35/34964706/6337.593 · 35/26793443/13019.120 · 17/23796600/1170.211 · 13/18197400/261.358 | holds: busiest (35, 2 tied) holds both |
| 2 | A | 1 | 2000000 | tokio | 3 | no | 9890.169 | 7092.568 | 1.1 | missed slots, lateness ratio, steady state | 30/26019285/10972.299 · 29/25059124/6228.541 · 21/29395800/27.787 · 20/27217219/3535.798 | fails: busiest (30, 1 tied): not lowest delivered/connection |
| 2 | A | 1 | 2000000 | tokio | 4 | no | 8707.375 | 6048.186 | 1.5 | missed slots, lateness ratio, steady state | 29/25759775/6480.200 · 28/34975481/3890.217 · 27/24220142/9915.335 · 16/22396800/128.254 | fails: busiest (29, 1 tied): not highest p99 |
| 2 | A | 1 | 2000000 | tokio | 5 | no | 7193.231 | 3244.294 | 1.3 | missed slots, lateness ratio, steady state | 29/24209103/8145.338 · 26/25009309/5410.652 · 23/27329190/4127.195 · 22/30795600/111.018 | holds: busiest (29, 1 tied) holds both |

### Experiment 2 verdict: unexplained

Q7 rule with the Q8 tie rule: explained when, in every invalid 4-worker run (experiment 2 plus every
4-worker run of experiment 1), the busiest worker has both the lowest delivered-per-connection and
the highest per-worker p99. With a tie at the top, any one tied worker may satisfy both conditions.
Otherwise unexplained.

- Invalid 4-worker runs: 61 (experiment 1: uring 8 of 40, tokio twin 13 of 40; experiment 2: uring
  20 of 20, tokio twin 20 of 20).
- The rule holds in 38 of 61: uring 17 of 28, tokio twin 21 of 33. The verdict is **unexplained**.
- The 23 failures split as follows (from the rule column): the busiest worker is not the lowest
  delivered-per-connection worker, or does not have the highest per-worker p99, or both; in one run
  (experiment 2, uring 2 000 000 rep 3) two workers tie at the top and neither holds both.

#### Observations (not applied)

These observations come from the data. No rule uses them.

- In all 59 valid 4-worker runs, delivered-per-connection is identical on every worker (relative
  spread 0). In the invalid runs, the relative spread has a median of 10 % and a maximum of 45 %. The
  shortfall concentrates on some workers.
- In 48 of the 61 invalid runs, the worker with the highest per-worker p99 is also the worker with the
  lowest delivered-per-connection. The busiest worker has the highest per-worker p99 in 45 of 61 runs
  and the lowest delivered-per-connection in 45 of 61 runs.
- Two invalid 4-worker runs failed on the lateness-ratio row alone and have no delivery shortfall
  (experiment 1 B rep 2 uring w4 q0 1 300 000; experiment 1 B rep 5 uring w4 q0 1 500 000). The rule
  fails in the first and holds in the second.
- At 1 500 000 msgs/s and above, uring's generator-lateness p99 is 1.8-18.7 s, 59-95 % of the latency
  p99. The tokio twin's generator-lateness p99 at 1 300 000 stays at 2.6-6.9 ms. This session does not
  test why the generator falls behind in the uring runs.

## Lateness evidence at small latency p99 (evidence only)

This section is evidence for the 2026-10-27 Open row (EPIC-PLAN `## Remaining to close` row 2:
whether the 0.10 generator-lateness ratio stays a session stop at small latency p99). It rules
nothing. One row per configuration; the experiment 1 rows pool both arms (10 runs). "Ratio" is
generator-lateness p99 divided by latency p99 (limit 0.10). "Invalid on lateness alone" counts runs
whose only failed reconcile row is the lateness ratio.

| Exp | Workers | QoS | Rate | Server | Runs | Latency p99 (ms) min-max | Lateness p99 (ms) min-max | Ratio min-max | Ratio > 0.10 | Invalid on lateness alone |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 1 | 75000 | uring | 10 | 0.675-1.010 | 0.063-0.086 | 0.083-0.100 | 1 | 1 |
| 1 | 1 | 1 | 75000 | tokio | 10 | 0.716-2.435 | 0.062-0.085 | 0.035-0.094 | 0 | 0 |
| 1 | 1 | 1 | 75000 | mosquitto | 10 | 13.730-2506.097 | 0.097-0.227 | 0.000-0.011 | 0 | 0 |
| 1 | 1 | 1 | 300000 | uring | 10 | 9.642-330.826 | 0.378-0.549 | 0.002-0.057 | 0 | 0 |
| 1 | 1 | 1 | 300000 | tokio | 10 | 57.573-2686.452 | 0.333-7.012 | 0.000-0.009 | 0 | 0 |
| 1 | 1 | 1 | 300000 | mosquitto | 10 | 36507.222-57914.950 | 1501.561-32833.012 | 0.027-0.582 | 3 | 0 |
| 1 | 1 | 0 | 150000 | uring | 10 | 4.379-7.156 | 0.155-0.348 | 0.032-0.068 | 0 | 0 |
| 1 | 1 | 0 | 150000 | tokio | 10 | 4.387-5.009 | 0.142-0.230 | 0.031-0.048 | 0 | 0 |
| 1 | 1 | 0 | 150000 | mosquitto | 10 | 21.807-2344.616 | 0.101-0.166 | 0.000-0.005 | 0 | 0 |
| 1 | 1 | 0 | 400000 | uring | 10 | 16.908-110.232 | 0.652-1.528 | 0.007-0.061 | 0 | 0 |
| 1 | 1 | 0 | 400000 | tokio | 10 | 70.779-197.001 | 0.542-1.281 | 0.003-0.018 | 0 | 0 |
| 1 | 1 | 0 | 400000 | mosquitto | 10 | 6522.143-18152.948 | 3886.023-13421.773 | 0.558-0.793 | 10 | 0 |
| 1 | 4 | 1 | 900000 | uring | 10 | 21.152-265.945 | 1.756-3.090 | 0.009-0.098 | 0 | 0 |
| 1 | 4 | 1 | 900000 | tokio | 10 | 28.869-4292.870 | 1.548-2.681 | 0.001-0.072 | 0 | 0 |
| 1 | 4 | 1 | 1200000 | uring | 10 | 62.751-4120.904 | 2.425-2340.422 | 0.004-0.568 | 1 | 0 |
| 1 | 4 | 1 | 1200000 | tokio | 10 | 745.538-8338.276 | 2.437-4.411 | 0.000-0.003 | 0 | 0 |
| 1 | 4 | 0 | 1300000 | uring | 10 | 18.891-318.243 | 1.629-2.572 | 0.006-0.101 | 1 | 1 |
| 1 | 4 | 0 | 1300000 | tokio | 10 | 30.835-3550.478 | 1.637-3.021 | 0.001-0.072 | 0 | 0 |
| 1 | 4 | 0 | 1500000 | uring | 10 | 33.325-3269.460 | 2.329-2079.326 | 0.004-0.636 | 3 | 1 |
| 1 | 4 | 0 | 1500000 | tokio | 10 | 64.848-4890.558 | 2.165-1910.505 | 0.001-0.391 | 1 | 0 |
| 2 | 4 | 1 | 1300000 | uring | 5 | 2126.512-5104.468 | 2.879-3932.160 | 0.001-0.770 | 3 | 0 |
| 2 | 4 | 1 | 1300000 | tokio | 5 | 5351.932-7042.236 | 2.640-6.926 | 0.000-0.001 | 0 | 0 |
| 2 | 4 | 1 | 1500000 | uring | 5 | 3061.842-20266.877 | 1799.356-18656.264 | 0.588-0.921 | 5 | 0 |
| 2 | 4 | 1 | 1500000 | tokio | 5 | 5242.880-6652.166 | 3.736-3594.519 | 0.001-0.600 | 1 | 0 |
| 2 | 4 | 1 | 1700000 | uring | 5 | 4550.820-17129.538 | 3615.490-15753.806 | 0.768-0.920 | 5 | 0 |
| 2 | 4 | 1 | 1700000 | tokio | 5 | 4777.312-8501.854 | 4.256-5368.709 | 0.001-0.631 | 4 | 0 |
| 2 | 4 | 1 | 2000000 | uring | 5 | 4454.351-15762.194 | 3445.621-14914.945 | 0.774-0.952 | 5 | 0 |
| 2 | 4 | 1 | 2000000 | tokio | 5 | 5007.999-11794.383 | 1356.857-8527.020 | 0.271-0.723 | 5 | 0 |

- At the smallest latency p99 (w1 q1 75 000, uring and the tokio twin, p99 0.68-2.4 ms), the
  generator-lateness p99 is 0.062-0.086 ms and the ratio is 0.035-0.100. One of these 20 runs failed
  on the ratio alone: experiment 1 A rep 2 uring, lateness p99 67 839 ns against latency p99
  675 327 ns (ratio 0.1005).
- At w1 q0 150 000 (p99 4.4-7.2 ms), the ratio is 0.031-0.068 in all 20 runs.
- At 4 workers, the generator-lateness p99 of the 59 valid runs is 1.5-6.8 ms. One 4-worker run failed on the ratio alone at small latency p99:
  experiment 1 B rep 2 uring w4 q0 1 300 000, lateness p99 1 904 639 ns against latency p99
  18 890 751 ns (ratio 0.1008).
- Three of the 240 runs failed on the ratio alone. The third (experiment 1 B rep 5 uring w4 q0
  1 500 000) has a latency p99 of 1 051 ms and a lateness p99 of 121 ms (ratio 0.115), so it is not a
  small-p99 case.
- The magnitudes agree with `bench/CALIBRATION.md` finding 2 (0.04-0.7 ms at 1 worker, 0.7-2 ms at
  4 workers).

## Raw data

The raw JSON of all 240 runs, valid or invalid, stays on the owner's workstation in `bench/diagnosis/`,
with each `.scratch/` directory, the per-invocation `.stdout` and `.stderr` files, and `session.log`.
`bench/.gitignore` excludes the directory, so it is not committed. The tables and scripts in this file are the committed record.

- `exp1/A/rep1/` … `exp1/B/rep5/`: 20 records each (experiment 1).
- `exp2/run/`: 40 records (experiment 2).
- Size: 542 368 113 bytes (520 MB) in total. The 240 run records hold 282 624 701 bytes (about 1.2 MB each). The 1 040 files in the `.scratch/` directories hold 259 702 920 bytes. The rest is the `.stdout`, `.stderr` and `session.log` files.

## Appendix

### Diagnosis-only patch (verbatim `git diff` of the worktree)

```diff
diff --git a/bench/src/matrix.rs b/bench/src/matrix.rs
index 3f8bd00..38d4faf 100644
--- a/bench/src/matrix.rs
+++ b/bench/src/matrix.rs
@@ -1026,6 +1026,9 @@ pub fn run_matrix_with<F: FnMut(&RunSpec) -> RunRecord>(
         .collect();
 
     let mut outcome = MatrixOutcome::default();
+    // DIAGNOSIS-ONLY (not committed): `DIAG_RATES` skips the throughput and
+    // connections stages and replaces each row's latency rates.
+    let diag = std::env::var("DIAG_RATES").ok();
 
     for &w in &spec.workers {
         let servers = servers_for(w, &spec.servers);
@@ -1054,6 +1057,10 @@ pub fn run_matrix_with<F: FnMut(&RunSpec) -> RunRecord>(
                     thr_skipped.push(key_of(&s));
                 }
             }
+            if diag.is_some() {
+                thr_specs.clear();
+                thr_skipped.clear();
+            }
             let thr_outcome = run_configs(
                 &thr_specs,
                 &remaining,
@@ -1071,10 +1078,22 @@ pub fn run_matrix_with<F: FnMut(&RunSpec) -> RunRecord>(
             let rates = declared_rates(spec.profile, w, q)
                 .ok_or_else(|| format!("no declared rates for workers {w} QoS {q}"))?;
 
+            let lat_rates: Vec<u64> = match diag.as_deref() {
+                None => rates.both().to_vec(),
+                Some("declared") => declared_rates(Profile::Recorded, w, q)
+                    .ok_or_else(|| format!("no recorded declared rates for workers {w} QoS {q}"))?
+                    .both()
+                    .to_vec(),
+                Some(list) => list
+                    .split(',')
+                    .map(|r| r.trim().parse::<u64>().map_err(|e| format!("DIAG_RATES {r:?}: {e}")))
+                    .collect::<Result<_, _>>()?,
+            };
+
             // Latency configurations.
             let mut lat_specs: Vec<RunSpec> = Vec::new();
             for &server in &servers {
-                for rate in rates.both() {
+                for rate in lat_rates.iter().copied() {
                     let mut s = RunSpec {
                         profile: spec.profile,
                         server,
@@ -1201,6 +1220,9 @@ pub fn run_matrix_with<F: FnMut(&RunSpec) -> RunRecord>(
                 outcome.passed.push(key_of(&s));
             }
         }
+        if diag.is_some() {
+            conn_specs.clear();
+        }
         let conn_outcome = run_configs(
             &conn_specs,
             &remaining,
diff --git a/bench/src/runner.rs b/bench/src/runner.rs
index cb92055..6947f43 100644
--- a/bench/src/runner.rs
+++ b/bench/src/runner.rs
@@ -390,6 +390,18 @@ impl ProtocolConstants {
                 max_config_attempts: 3,
                 enforce_spread: true,
             },
+            // DIAGNOSIS-ONLY (not committed): recorded timing under the smoke profile.
+            Profile::Smoke if std::env::var_os("DIAG_RATES").is_some() => Self {
+                warmup_secs: 10,
+                window_secs: 60,
+                repetitions: std::env::var("DIAG_REPS")
+                    .ok()
+                    .and_then(|r| r.parse().ok())
+                    .unwrap_or(5),
+                drain_grace_secs: 5,
+                max_config_attempts: 1,
+                enforce_spread: false,
+            },
             Profile::Smoke => Self {
                 warmup_secs: 2,
                 window_secs: 5,
```

### Driver script (`run.sh`, verbatim)

```bash
#!/usr/bin/env bash
# F4.3 diagnosis session driver. Runs experiment 1 (placement A/B) and
# experiment 2 (w4 shortfall) with the diagnosis-only patch built in the
# scratch worktree. Every step waits for host CPU pressure below 2 %.
set -u
BENCH=/home/alex/dev/7laps/uring-mqtt-diag/bench/target/release/bench
OUT=/home/alex/dev/7laps/uring-mqtt/bench/diagnosis
LOG="$OUT/session.log"
SERVER_CPUS=0,2,4,6
ARM_A=8-17
ARM_B=1,3,5,7,8-13
PRESSURE_LIMIT=2.00
PRESSURE_POLL_SECS=5
REPS=5

mkdir -p "$OUT"
log() { echo "$(date -Is) $*" | tee -a "$LOG"; }

avg10() { awk '/^some/ { sub("avg10=", "", $2); print $2 }' /proc/pressure/cpu; }

wait_quiet() {
  while :; do
    local p; p=$(avg10)
    if awk -v p="$p" -v l="$PRESSURE_LIMIT" 'BEGIN { exit !(p < l) }'; then
      log "gate passed: some avg10=$p"
      return
    fi
    log "gate waiting: some avg10=$p"
    sleep "$PRESSURE_POLL_SECS"
  done
}

step() { # name out_dir rates reps generator_cpus servers workers qos
  local name=$1 dir=$2 rates=$3 reps=$4 gen=$5 servers=$6 workers=$7 qos=$8
  wait_quiet
  log "start $name: DIAG_RATES=$rates DIAG_REPS=$reps generator=$gen servers=$servers workers=$workers qos=$qos out=$dir"
  DIAG_RATES=$rates DIAG_REPS=$reps "$BENCH" run --profile smoke --out "$dir" \
    --servers "$servers" --workers "$workers" --qos "$qos" --syscalls none \
    --mosquitto-bin /usr/sbin/mosquitto \
    --server-cpus "$SERVER_CPUS" --generator-cpus "$gen" \
    >>"$dir.stdout" 2>>"$dir.stderr"
  local rc=$?
  log "end $name: exit=$rc"
  if [ "$rc" -eq 1 ]; then log "refusal, abort"; exit 1; fi
}

log "session start, HEAD $(git -C /home/alex/dev/7laps/uring-mqtt-diag rev-parse --short HEAD)"
mkdir -p "$OUT/exp1/A" "$OUT/exp1/B" "$OUT/exp2"

# Experiment 1: arms interleaved per repetition, order alternating (AB, BA, ...).
for rep in $(seq 1 "$REPS"); do
  if [ $((rep % 2)) -eq 1 ]; then order="A B"; else order="B A"; fi
  for arm in $order; do
    if [ "$arm" = A ]; then gen=$ARM_A; else gen=$ARM_B; fi
    step "exp1 arm $arm rep $rep" "$OUT/exp1/$arm/rep$rep" declared 1 "$gen" \
      uring,tokio,mosquitto 1,4 1,0
  done
done

# Experiment 2: w4 q1 rate ladder, default placement, 5 repetitions in one invocation.
step "exp2" "$OUT/exp2/run" 1300000,1500000,1700000,2000000 "$REPS" "$ARM_A" uring,tokio 4 1

log "session end"
```

### Analysis script (`analyze.py`, verbatim)

```python
"""F4.3 diagnosis analysis. Reads the raw records under bench/diagnosis/ and
prints the DIAGNOSIS.md tables and the two verdicts under the rules
pre-registered in features/bench-worker-loads-record/RESEARCH.md Q6-Q8.

Usage: python3 -I analyze.py <bench/diagnosis dir>
"""
import glob
import json
import os
import sys

ROOT = sys.argv[1]
ARMS = {"A": "8-17", "B": "1,3,5,7,8-13"}
REPS = 5
VALID_COUNT_GAP = 4      # Q6: valid-repetition counts differ by 4 or more
SENSITIVE_MIN = 2        # Q6: 2 or more configurations differ in the same direction
LATENESS_FRACTION = 0.10  # GENERATOR_LATENESS_FRACTION, evidence only here
SERVER_ORDER = {"uring": 0, "tokio": 1, "mosquitto": 2}
ROW_ORDER = [(1, 1), (1, 0), (4, 1), (4, 0)]

# Derived count rows are omitted from "failed rows", as in bench/CALIBRATION.md.
ROWS = [
    ("missed slots", "missed slots"),
    ("generator lateness p99", "lateness ratio"),
    ("generator lateness over range", "lateness over range"),
    ("backlog grew", "steady state"),
    ("unresolved", "unresolved"),
    ("stalled", "stalled"),
    ("unacknowledged", "unacknowledged"),
    ("not sustained", "not sustained"),
    ("dropped", "dropped"),
    ("duplicates", "duplicates"),
    ("protocol errors", "protocol errors"),
    ("connect errors", "connect errors"),
    ("driver proof", "driver proof"),
    ("latency samples over range", "latency over range"),
    ("clock anomalies", "clock anomalies"),
    ("no latency samples", "no latency samples"),
]
DERIVED = ("scheduled ", "latency samples ", "lateness samples ")


def failed_rows(reasons):
    out = []
    for r in reasons:
        label = next((lab for key, lab in ROWS if r.startswith(key)), None)
        if label is None:
            if r.startswith(DERIVED) and "over range" not in r:
                continue
            label = "other: " + r[:60]
        if label not in out:
            out.append(label)
    return out


def pressure_pct(rec):
    p = rec.get("host_cpu_pressure")
    if not p or not p.get("sampled_ns"):
        return None
    return p["some_stall_us"] * 1000 / p["sampled_ns"] * 100


def pct(p):
    return "—" if p is None else f"{p:.1f}"


def ms(ns):
    return "—" if ns is None else f"{ns / 1e6:.3f}"


def load(path, exp, arm, rep_from_dir):
    rec = json.load(open(path))
    rep = rep_from_dir if rep_from_dir is not None else rec["repetition"]
    lat = rec.get("latency_ns") or {}
    return {
        "exp": exp,
        "arm": arm,
        "rep": rep,
        "file": os.path.relpath(path, ROOT),
        "server": rec["server"],
        "workers": rec["workers"],
        "qos": rec["qos"],
        "rate": rec["offered_rate"],
        "valid": rec["valid"],
        "p99": lat.get("p99"),
        "lateness_p99": rec.get("lateness_p99_ns"),
        "pressure": pressure_pct(rec),
        "rows": failed_rows(rec["invalid_reasons"]),
        "loads": rec.get("worker_loads"),
        "gen_cpus": rec["cpu_sets"]["generator"],
        "cancelled": rec.get("cancelled", False),
        "harness_errors": rec.get("harness_errors", []),
    }


def records():
    out = []
    for arm in ARMS:
        for rep in range(1, REPS + 1):
            for f in sorted(glob.glob(f"{ROOT}/exp1/{arm}/rep{rep}/*.json")):
                out.append(load(f, 1, arm, rep))
    for f in sorted(glob.glob(f"{ROOT}/exp2/run/*.json")):
        out.append(load(f, 2, "A", None))
    return out


def cfg_key(r):
    return (ROW_ORDER.index((r["workers"], r["qos"])), r["rate"], SERVER_ORDER[r["server"]])


def p99_list(runs):
    return [r["p99"] for r in sorted(runs, key=lambda r: r["rep"])]


def fmt_list(values, f):
    return ", ".join(f(v) for v in values)


def differs(a_runs, b_runs):
    """Q6: returns (direction, reason) or None. Direction names the worse arm."""
    found = []
    pa, pb = p99_list(a_runs), p99_list(b_runs)
    if None not in pa and None not in pb and pa and pb:
        if min(pa) > max(pb):
            found.append(("A", "every A p99 > every B p99"))
        elif min(pb) > max(pa):
            found.append(("B", "every B p99 > every A p99"))
    va = sum(r["valid"] for r in a_runs)
    vb = sum(r["valid"] for r in b_runs)
    if abs(va - vb) >= VALID_COUNT_GAP:
        found.append(("A" if va < vb else "B", f"valid {va} vs {vb}"))
    return found


def loads_cell(loads):
    if not loads:
        return "null"
    return " · ".join(f"{w['connections']}/{w['delivered']}/{ms(w['latency_p99_ns'])}" for w in loads)


def rule_holds(loads):
    """Q7 with the Q8 tie rule. Workers with no connection have no
    delivered-per-connection and are left out of the minimum."""
    if not loads:
        return None, "no worker_loads"
    top = max(w["connections"] for w in loads)
    if top == 0:
        return None, "no connection attributed"
    per_conn = [w["delivered"] / w["connections"] for w in loads if w["connections"] > 0]
    p99s = [w["latency_p99_ns"] for w in loads if w["latency_p99_ns"] is not None]
    lowest = min(per_conn)
    highest = max(p99s) if p99s else None
    busiest = [w for w in loads if w["connections"] == top]
    for w in busiest:
        dpc = w["delivered"] / w["connections"]
        if dpc <= lowest and w["latency_p99_ns"] is not None and w["latency_p99_ns"] >= highest:
            return True, f"busiest ({top}, {len(busiest)} tied) holds both"
    parts = []
    if not any(w["delivered"] / w["connections"] <= lowest for w in busiest):
        parts.append("not lowest delivered/connection")
    if not any(w["latency_p99_ns"] is not None and w["latency_p99_ns"] >= highest for w in busiest):
        parts.append("not highest p99")
    if not parts:
        parts.append("no single tied worker holds both")
    return False, f"busiest ({top}, {len(busiest)} tied): " + ", ".join(parts)


def main():
    recs = records()
    bad = [r for r in recs if r["cancelled"] or r["harness_errors"]]
    print(f"records: {len(recs)} (exp1 {sum(r['exp'] == 1 for r in recs)}, exp2 {sum(r['exp'] == 2 for r in recs)}); cancelled or harness errors: {len(bad)}")
    for r in bad:
        print(f"  {r['file']}: cancelled={r['cancelled']} harness_errors={r['harness_errors']}")
    for r in recs:
        if r["exp"] == 1 and r["gen_cpus"] != ARMS[r["arm"]]:
            print(f"  ARM MISMATCH {r['file']}: {r['gen_cpus']}")

    # Experiment 1 table and verdict.
    e1 = [r for r in recs if r["exp"] == 1]
    configs = sorted({cfg_key(r): (r["server"], r["workers"], r["qos"], r["rate"]) for r in e1}.items())
    print("\n### Experiment 1 per configuration\n")
    print("| Workers | QoS | Rate (msgs/s) | Server | Arm | Repetition p99 (ms), rep 1-5 | Valid | Lateness p99 (ms), rep 1-5 | Pressure (%), rep 1-5 | Failed rows (rep: rows) | Differs (Q6) |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    diffs = []
    for _, (server, w, q, rate) in configs:
        sel = lambda arm: sorted((r for r in e1 if r["arm"] == arm and r["server"] == server and r["workers"] == w and r["qos"] == q and r["rate"] == rate), key=lambda r: r["rep"])
        a, b = sel("A"), sel("B")
        d = differs(a, b)
        if d:
            diffs.append(((server, w, q, rate), d))
        dcell = "; ".join(f"{reason} (worse: {arm})" for arm, reason in d) or "no"
        for arm, runs in (("A", a), ("B", b)):
            fr = "; ".join(f"{r['rep']}: {', '.join(r['rows'])}" for r in runs if not r["valid"]) or ""
            print(f"| {w} | {q} | {rate} | {server} | {arm} `{ARMS[arm]}` | {fmt_list(p99_list(runs), ms)} | {sum(r['valid'] for r in runs)}/{len(runs)} | {fmt_list([r['lateness_p99'] for r in runs], ms)} | {fmt_list([r['pressure'] for r in runs], pct)} | {fr} | {dcell if arm == 'A' else ''} |")
    print("\n### Experiment 1 verdict input\n")
    for cfg, d in diffs:
        print(f"- {cfg}: {d}")
    dirs = {}
    for cfg, d in diffs:
        for arm in {arm for arm, _ in d}:
            dirs.setdefault(arm, []).append(cfg)
    if not diffs:
        verdict = "robust"
    elif any(len(v) >= SENSITIVE_MIN for v in dirs.values()):
        verdict = "sensitive"
    else:
        verdict = "inconclusive"
    print(f"configurations that differ: {len(diffs)}; by worse arm: { {k: len(v) for k, v in dirs.items()} }")
    print(f"EXPERIMENT 1 VERDICT: {verdict}")

    # Experiment 2 (and every w4 run of experiment 1): per-run worker loads.
    w4 = sorted((r for r in recs if r["workers"] == 4), key=lambda r: (r["exp"], r["qos"] != 1, r["rate"], SERVER_ORDER[r["server"]], r["arm"], r["rep"]))
    print("\n### w4 runs with worker_loads\n")
    print("worker_loads cell: connections/delivered/p99 ms per worker, busiest first.\n")
    print("| Exp | Arm | QoS | Rate (msgs/s) | Server | Rep | Valid | p99 (ms) | Lateness p99 (ms) | Pressure (%) | Failed rows | worker_loads | Q7 rule (invalid runs) |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    invalid_results = []
    for r in w4:
        rule = ""
        if not r["valid"]:
            holds, why = rule_holds(r["loads"])
            invalid_results.append((r, holds))
            rule = ("holds" if holds else "fails" if holds is False else "n/a") + f": {why}"
        print(f"| {r['exp']} | {r['arm']} | {r['qos']} | {r['rate']} | {r['server']} | {r['rep']} | {'yes' if r['valid'] else 'no'} | {ms(r['p99'])} | {ms(r['lateness_p99'])} | {pct(r['pressure'])} | {', '.join(r['rows'])} | {loads_cell(r['loads'])} | {rule} |")
    n_inv = len(invalid_results)
    n_hold = sum(1 for _, h in invalid_results if h)
    print(f"\ninvalid w4 runs: {n_inv}; rule holds in {n_hold}")
    for server in ("uring", "tokio"):
        sub = [(r, h) for r, h in invalid_results if r["server"] == server]
        print(f"  {server}: invalid {len(sub)}, holds {sum(1 for _, h in sub if h)}")
    if n_inv == 0:
        v2 = "unexplained (no invalid run to test)"
    elif n_hold == n_inv:
        v2 = "explained"
    else:
        v2 = "unexplained"
    print(f"EXPERIMENT 2 VERDICT: {v2}")

    # Evidence only: generator lateness against latency p99 (2026-10-27 Open row).
    print("\n### Lateness evidence (every run, sorted by latency p99)\n")
    print("| Exp | Arm | Server | Workers | QoS | Rate | Rep | p99 (ms) | Lateness p99 (ms) | Ratio | Valid |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for r in sorted(recs, key=lambda r: (r["p99"] is None, r["p99"] or 0)):
        ratio = (r["lateness_p99"] / r["p99"]) if r["p99"] and r["lateness_p99"] is not None else None
        print(f"| {r['exp']} | {r['arm']} | {r['server']} | {r['workers']} | {r['qos']} | {r['rate']} | {r['rep']} | {ms(r['p99'])} | {ms(r['lateness_p99'])} | {'—' if ratio is None else f'{ratio:.3f}'} | {'yes' if r['valid'] else 'no'} |")
    over = [r for r in recs if r["p99"] and r["lateness_p99"] is not None and r["lateness_p99"] > LATENESS_FRACTION * r["p99"]]
    print(f"\nruns with lateness ratio above {LATENESS_FRACTION}: {len(over)}")
    for r in over:
        print(f"  {r['file']}: p99 {ms(r['p99'])} ms, lateness {ms(r['lateness_p99'])} ms, rows {r['rows']}")


main()
```

### Evidence-only scripts

The lateness summary and the experiment 2 per-configuration table come from these scripts. They
apply no rule. `summary.py` also printed the observation counts above.

```python
"""Evidence-only summaries for DIAGNOSIS.md. Not a decision rule."""
import glob, json, os, sys
from collections import defaultdict
ROOT = sys.argv[1]
recs = []
for f in sorted(glob.glob(f"{ROOT}/exp*/**/*.json", recursive=True)):
    if "/.scratch/" in f:
        continue
    r = json.load(open(f))
    rel = os.path.relpath(f, ROOT)
    parts = rel.split("/")
    exp = parts[0]
    arm = parts[1] if exp == "exp1" else "A"
    rep = int(parts[2][3:]) if exp == "exp1" else r["repetition"]
    lat = (r.get("latency_ns") or {}).get("p99")
    late = r.get("lateness_p99_ns")
    reasons = r["invalid_reasons"]
    late_row = any(x.startswith("generator lateness p99") for x in reasons)
    other = [x for x in reasons if not x.startswith(("generator lateness p99", "scheduled ", "latency samples ", "lateness samples ", "host CPU pressure"))]
    p = r.get("host_cpu_pressure")
    pr = p["some_stall_us"] * 1000 / p["sampled_ns"] * 100 if p and p.get("sampled_ns") else None
    recs.append(dict(exp=exp, arm=arm, rep=rep, server=r["server"], w=r["workers"], q=r["qos"], rate=r["offered_rate"],
                     valid=r["valid"], p99=lat, late=late, late_row=late_row, late_only=late_row and not other,
                     loads=r.get("worker_loads"), pressure=pr, dirty=r.get("env", {}).get("git_dirty"),
                     commit=r.get("env", {}).get("git_commit") or r.get("env", {}).get("commit"), file=rel,
                     started=r.get("started_at") or r.get("started")))
print("env keys:", sorted(json.load(open(os.path.join(ROOT, recs[0]["file"])))["env"].keys()))
print("git_dirty values:", {r["dirty"] for r in recs}, "commits:", {r["commit"] for r in recs})
pr = [r["pressure"] for r in recs if r["pressure"] is not None]
print(f"pressure range {min(pr):.1f}-{max(pr):.1f}; w1 max {max(r['pressure'] for r in recs if r['w']==1):.1f}")
zero = sum(1 for r in recs if r["loads"] for w in r["loads"] if w["connections"] == 0)
print("zero-connection workers:", zero, "; w4 runs without loads:", sum(1 for r in recs if r["w"] == 4 and not r["loads"]))
print("connections per w4 run:", {sum(w["connections"] for w in r["loads"]) for r in recs if r["w"] == 4})
print("w1 loads:", {len(r["loads"]) if r["loads"] else None for r in recs if r["w"] == 1})
groups = defaultdict(list)
for r in recs:
    groups[(r["exp"], r["w"], r["q"], r["rate"], r["server"])].append(r)
order = {"uring": 0, "tokio": 1, "mosquitto": 2}
print("\n| Exp | Workers | QoS | Rate | Server | Runs | Latency p99 (ms) min-max | Lateness p99 (ms) min-max | Ratio min-max | Ratio > 0.10 | Invalid on lateness alone |")
print("|---|---|---|---|---|---|---|---|---|---|---|")
for k in sorted(groups, key=lambda k: (k[0], k[1], -k[2], k[3], order[k[4]])):
    g = groups[k]
    ps = [r["p99"] / 1e6 for r in g]; ls = [r["late"] / 1e6 for r in g]
    rs = [r["late"] / r["p99"] for r in g]
    print(f"| {k[0][-1]} | {k[1]} | {k[2]} | {k[3]} | {k[4]} | {len(g)} | {min(ps):.3f}-{max(ps):.3f} | {min(ls):.3f}-{max(ls):.3f} | {min(rs):.3f}-{max(rs):.3f} | {sum(x > 0.10 for x in rs)} | {sum(r['late_only'] for r in g)} |")
# Observations on w4 invalid runs (not a rule).
inv = [r for r in recs if r["w"] == 4 and not r["valid"]]
def stats(r):
    L = r["loads"]
    top = max(w["connections"] for w in L)
    dpc = {i: w["delivered"] / w["connections"] for i, w in enumerate(L) if w["connections"]}
    lo = min(dpc.values()); hi = max(w["latency_p99_ns"] for w in L)
    busiest = [i for i, w in enumerate(L) if w["connections"] == top]
    hi_idx = [i for i, w in enumerate(L) if w["latency_p99_ns"] == hi]
    lo_idx = [i for i, v in dpc.items() if v == lo]
    return dict(b_hi=any(i in hi_idx for i in busiest), b_lo=any(i in lo_idx for i in busiest),
                same=bool(set(hi_idx) & set(lo_idx)), spread=(max(dpc.values()) - lo) / max(dpc.values()))
S = [stats(r) for r in inv]
n = len(S)
print(f"\ninvalid w4 runs {n}: busiest has highest p99 {sum(s['b_hi'] for s in S)}; busiest has lowest dpc {sum(s['b_lo'] for s in S)}; highest-p99 worker == lowest-dpc worker {sum(s['same'] for s in S)}")
print("lateness-alone invalid w4 runs:", [r["file"] for r in inv if r["late_only"]])
sp = sorted(s["spread"] for s in S)
print(f"dpc relative spread in invalid runs: min {sp[0]:.5f} median {sp[len(sp)//2]:.5f} max {sp[-1]:.5f}")
val = [r for r in recs if r["w"] == 4 and r["valid"]]
spv = sorted(stats(r)["spread"] for r in val)
print(f"dpc relative spread in valid w4 runs ({len(val)}): min {spv[0]:.5f} median {spv[len(spv)//2]:.5f} max {spv[-1]:.5f}")
for srv in ("uring", "tokio"):
    for e in ("exp1", "exp2"):
        sub = [r for r in recs if r["w"] == 4 and r["server"] == srv and r["exp"] == e]
        print(srv, e, "runs", len(sub), "invalid", sum(not r["valid"] for r in sub))
```

```python
"""Per-configuration view of experiment 2, same cells as the experiment 1 table."""
import glob, json, sys
ROOT = sys.argv[1]
rows = {}
for f in sorted(glob.glob(f"{ROOT}/exp2/run/*.json")):
    r = json.load(open(f))
    rows.setdefault((r["offered_rate"], r["server"] != "uring", r["server"]), []).append(r)
ms = lambda v: f"{v / 1e6:.3f}"
def pr(r):
    p = r["host_cpu_pressure"]; return f"{p['some_stall_us'] * 1000 / p['sampled_ns'] * 100:.1f}"
print("| Workers | QoS | Rate (msgs/s) | Server | Arm | Repetition p99 (ms), rep 1-5 | Valid | Lateness p99 (ms), rep 1-5 | Pressure (%), rep 1-5 |")
print("|---|---|---|---|---|---|---|---|---|")
for (rate, _, server), rs in sorted(rows.items()):
    rs.sort(key=lambda r: r["repetition"])
    assert [r["repetition"] for r in rs] == [1, 2, 3, 4, 5]
    print(f"| 4 | 1 | {rate} | {server} | A `8-17` | {', '.join(ms(r['latency_ns']['p99']) for r in rs)} | {sum(r['valid'] for r in rs)}/5 | {', '.join(ms(r['lateness_p99_ns']) for r in rs)} | {', '.join(pr(r) for r in rs)} |")
```

### Host facts

`lscpu -e` (guest):

```text
CPU NODE SOCKET CORE L1d:L1i:L2:L3 ONLINE
  0    0      0    0 0:0:0:0          yes
  1    0      0    0 0:0:0:0          yes
  2    0      0    1 1:1:1:0          yes
  3    0      0    1 1:1:1:0          yes
  4    0      0    2 2:2:2:0          yes
  5    0      0    2 2:2:2:0          yes
  6    0      0    3 3:3:3:0          yes
  7    0      0    3 3:3:3:0          yes
  8    0      0    4 4:4:4:0          yes
  9    0      0    4 4:4:4:0          yes
 10    0      0    5 5:5:5:0          yes
 11    0      0    5 5:5:5:0          yes
 12    0      0    6 6:6:6:0          yes
 13    0      0    6 6:6:6:0          yes
 14    0      0    7 7:7:7:0          yes
 15    0      0    7 7:7:7:0          yes
 16    0      0    8 8:8:8:0          yes
 17    0      0    8 8:8:8:0          yes
```

`lscpu` (guest):

```text
Architecture:                            x86_64
CPU op-mode(s):                          32-bit, 64-bit
Address sizes:                           39 bits physical, 48 bits virtual
Byte Order:                              Little Endian
CPU(s):                                  18
On-line CPU(s) list:                     0-17
Vendor ID:                               GenuineIntel
Model name:                              Intel(R) Core(TM) i7-14650HX
CPU family:                              6
Model:                                   183
Thread(s) per core:                      2
Core(s) per socket:                      9
Socket(s):                               1
Stepping:                                1
BogoMIPS:                                4838.39
Flags:                                   fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ss ht syscall nx pdpe1gb rdtscp lm constant_tsc rep_good nopl xtopology tsc_reliable nonstop_tsc cpuid tsc_known_freq pni pclmulqdq vmx ssse3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt tsc_deadline_timer aes xsave avx f16c rdrand hypervisor lahf_lm abm 3dnowprefetch ssbd ibrs ibpb stibp ibrs_enhanced tpr_shadow ept vpid ept_ad fsgsbase tsc_adjust bmi1 avx2 smep bmi2 erms invpcid rdseed adx smap clflushopt clwb sha_ni xsaveopt xsavec xgetbv1 xsaves avx_vnni vnmi umip waitpkg gfni vaes vpclmulqdq rdpid movdiri movdir64b fsrm md_clear serialize ibt flush_l1d arch_capabilities
Virtualization:                          VT-x
Hypervisor vendor:                       Microsoft
Virtualization type:                     full
L1d cache:                               432 KiB (9 instances)
L1i cache:                               288 KiB (9 instances)
L2 cache:                                18 MiB (9 instances)
L3 cache:                                30 MiB (1 instance)
NUMA node(s):                            1
NUMA node0 CPU(s):                       0-17
Vulnerability Gather data sampling:      Not affected
Vulnerability Ghostwrite:                Not affected
Vulnerability Indirect target selection: Not affected
Vulnerability Itlb multihit:             Not affected
Vulnerability L1tf:                      Not affected
Vulnerability Mds:                       Not affected
Vulnerability Meltdown:                  Not affected
Vulnerability Mmio stale data:           Not affected
Vulnerability Old microcode:             Not affected
Vulnerability Reg file data sampling:    Mitigation; Clear Register File
Vulnerability Retbleed:                  Mitigation; Enhanced IBRS
Vulnerability Spec rstack overflow:      Not affected
Vulnerability Spec store bypass:         Mitigation; Speculative Store Bypass disabled via prctl
Vulnerability Spectre v1:                Mitigation; usercopy/swapgs barriers and __user pointer sanitization
Vulnerability Spectre v2:                Mitigation; Enhanced / Automatic IBRS; IBPB conditional; PBRSB-eIBRS SW sequence; BHI BHI_DIS_S
Vulnerability Srbds:                     Not affected
Vulnerability Tsa:                       Not affected
Vulnerability Tsx async abort:           Not affected
Vulnerability Vmscape:                   Not affected
```

`uname -r`: `6.18.33.2-microsoft-standard-WSL2`. `rustc --version`: `rustc 1.100.0-nightly (17fd5b8a3 2026-08-28)`. `mosquitto -h`: `mosquitto version 2.0.18`.
