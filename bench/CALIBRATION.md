# Declared-rate calibration

Spec §9.1 ("Declared rates", "Steady state", "Smoke gate") gives each row group (worker count × QoS)
two absolute offered rates, committed as named constants before any recorded run, and orders the
choice to come from a calibration whose method and results are committed. This file is that record.
The constants live in `bench/src/runner.rs` (`DECLARED_RATES_W1_Q1`, `DECLARED_RATES_W1_Q0`,
`DECLARED_RATES_W4_Q1`, `DECLARED_RATES_W4_Q0`, `SMOKE_DECLARED_RATES`) and the steady-state limit in
`bench/src/analysis.rs` (`STEADY_STATE_GROWTH_LIMIT_NS`). Changing any of them requires a fresh
calibration recorded here and a fresh recorded dataset.

## Method

- **Source:** commit `74298a4` in a scratch `git worktree`, with a calibration-only patch that is not
  committed: under `bench run --profile smoke`, an env var `CAL_RATES=<r1,r2,…>` switched the smoke
  timing to the recorded timing (10 s warm-up, 60 s window, 5 s drain), set the repetition count from
  `CAL_REPS`, skipped the throughput and connections stages, and dispatched one latency configuration
  per listed absolute rate for every server of the row. `SMOKE_CAL_RATES` did the same under the
  smoke timing (2 s warm-up, 5 s window, 2 s drain) with `SMOKE_REPS` repetitions. Every run went
  through the unmodified `run_one`, `reconcile` and record writer, so each record reconciles exactly
  as a recorded run would. The steady-state statistic was computed offline from the records' own
  `timeline`, `offered_rate`, `origin_ns` and `window` fields with the formula the steady-state row
  now implements.
- **Builds:** release for every recorded-profile rate; debug for the smoke rates (the smoke gate runs
  the debug binary).
- **Host:** the owner's WSL2 workstation (i7-14650HX, 18 logical CPUs, kernel
  `6.18.33.2-microsoft-standard-WSL2`), server CPUs `0,2,4,6`, generator CPUs `8-17`, an Android
  emulator (`qemu-system-x86_64-headless`) idling at about 8 % of one CPU. Before each step the
  script waited until `/proc/pressure/cpu` `some avg10` was below 2 %. Pressure per run is in the
  table (0.1-3.9 %; the higher values are the benchmark's own 4-worker runs).
- **Dates:** 2026-10-27, 01:06-05:15 local. An earlier attempt on 2026-10-23 was discarded: an
  unrelated workload (eleven `grid.py` processes) held host CPU pressure at 46-72 %.
- **Steps:** a 1-repetition screen over a coarse ladder per row group; a 2-repetition fine ladder at
  4 workers; a 5-repetition confirmation of the 1-worker candidates; a 5-repetition smoke ladder on
  the debug build.

## Selection rule

"Sustains R" means every calibration repetition at R is valid, the steady-state row included.
Spread is not part of the rule: see finding 1 below.

- **1-worker rows:** the lower rate is the highest calibrated rate every server in the row (uring,
  tokio twin, mosquitto) sustains; the higher rate is the highest calibrated rate both uring and the
  tokio twin sustain.
- **4-worker rows** (no mosquitto, so "every server" and "both" coincide): the lower rate is the
  lowest calibrated rate both servers sustain — below it the generator-lateness row fails (finding 2)
  — and the higher rate is the highest calibrated rate both sustain. At 4 workers QoS 0 this rule
  names a single rate, 1 300 000, for both: uring failed the lateness ratio once at 1 200 000, and
  1 400 000 was not calibrated. The higher rate there is 1 500 000, the next calibrated rate: tokio
  was valid in all 3 runs at it and uring in 2 of 3, and uring's one failure was missed slots, a
  capacity row. A recorded session that repeats that failure publishes uring "not sustained at
  1 500 000" for this row group instead of stopping.
- **Smoke:** the lowest pair of calibrated rates at which uring and the tokio twin are valid in every
  repetition with the lateness ratio at most 0.03 and no backlog growth, applied to every smoke row
  group. The smoke gate (`bench/README.md` `## Smoke`, `cli_run::smoke_matrix_writes_every_configuration_valid`)
  runs the 1-worker QoS 1 row.

## Declared rates

| Row group | Lower (msgs/s) | Higher (msgs/s) | Evidence |
|---|---|---|---|
| 1 worker, QoS 1 | 75 000 | 300 000 | every server 5/5 at 75 000 (mosquitto 100 000: unresolved); uring and tokio 5/5 at 300 000 (tokio 400 000: missed slots) |
| 1 worker, QoS 0 | 150 000 | 400 000 | every server 5/5 at 150 000 (mosquitto 200 000: backlog growth 4.6 s); uring and tokio 5/5 at 400 000 (600 000: growth 114-137 ms) |
| 4 workers, QoS 1 | 900 000 | 1 200 000 | both 2/2 at 900 000-1 200 000; 750 000 and below: lateness ratio; tokio 1 250 000: growth 545 ms |
| 4 workers, QoS 0 | 1 300 000 | 1 500 000 | both 2/2 at 1 300 000; uring 1 200 000: lateness ratio 1 of 2; uring 1 500 000: valid 2 of 3 (missed slots once) |
| Smoke (debug), every row group | 40 000 | 60 000 | uring and tokio 5/5 at both, lateness ratio ≤ 0.03, growth ≤ 0 (20 000: ratio up to 0.10; 10 000 and below: 0/5, lateness ratio) |

## Steady-state limit

`STEADY_STATE_GROWTH_LIMIT_NS` is 50 ms in both profiles. Across every calibration run that did not
grow a backlog, growth was −36 to +9.1 ms. Runs that passed every other row while their backlog grew
through the window measured +114 ms (tokio 1 worker QoS 0 at 600 000), +137 ms (uring, same), +545 ms
(tokio 4 workers QoS 1 at 1 250 000) and +4 631 ms (mosquitto 1 worker QoS 0 at 200 000, p99 5.1 s).
50 ms is 5.5 times the largest healthy value and below the smallest growing one.

## Findings that bound these rates

1. **Latency p99 spread.** Of the 16 configurations confirmed with 5 repetitions, every run was valid
   but only uring 1 worker QoS 0 at 150 000 kept its p99 spread within `SPREAD_LIMIT` (0.05); the
   others measured 0.12-1.74. Under §9.1 a configuration that exhausts its attempts on spread stops the
   session, so this calibration cannot select rates by spread. Routed to `/epic`.
2. **Generator lateness floor.** The generator's lateness p99 is 0.04-0.7 ms at 1 worker and
   0.7-2 ms at 4 workers in the release build, so the 0.10 lateness ratio fails whenever the latency
   p99 is small: at 4 workers every rate at or below 750 000 failed on lateness alone, and in the
   debug smoke build every rate at or below 10 000 did. Routed to `/epic`; the 4-worker magnitude is
   evidence for `preps/bench-open-loop-placement-and-w4-diagnosis.md`.

## Results

One row per run. `growth` is the steady-state statistic: backlog growth through the window as time at the offered rate (`window_scheduled − deliveries inside the window`, divided by the rate). `lateness ratio` is generator lateness p99 ÷ latency p99 (limit 0.10). `pressure` is host CPU pressure over the window. `failed rows` names the reconcile rows behind an invalid run; derived count rows (samples, scheduled) are omitted.

### Screen (release, 1 repetition)

| Server | Workers | QoS | Rate (msgs/s) | Rep | Valid | p50 (ms) | p99 (ms) | growth (ms) | lateness ratio | pressure (%) | failed rows |
|---|---|---|---|---|---|---|---|---|---|---|---|
| mosquitto | 1 | 0 | 50000 | 1 | yes | 0.381 | 0.909 | -0.5 | 0.05 | 0.3 |  |
| mosquitto | 1 | 0 | 100000 | 1 | yes | 0.757 | 3.559 | -0.5 | 0.03 | 0.5 |  |
| mosquitto | 1 | 0 | 150000 | 1 | yes | 1.795 | 25.510 | -2.8 | 0.00 | 0.4 |  |
| mosquitto | 1 | 0 | 200000 | 1 | yes | 3118.465 | 5075.108 | 4631.3 | 0.00 | 0.4 |  |
| tokio | 1 | 0 | 100000 | 1 | yes | 0.191 | 0.798 | -0.2 | 0.09 | 0.3 |  |
| tokio | 1 | 0 | 200000 | 1 | yes | 1.056 | 4.588 | -1.3 | 0.04 | 0.4 |  |
| tokio | 1 | 0 | 400000 | 1 | yes | 2.818 | 47.710 | -2.0 | 0.01 | 0.5 |  |
| tokio | 1 | 0 | 600000 | 1 | yes | 67.830 | 1598.030 | 113.8 | 0.00 | 0.5 |  |
| tokio | 1 | 0 | 800000 | 1 | no | 4659.872 | 22162.702 | 6590.4 | 0.86 | 0.5 | missed slots, lateness ratio |
| uring | 1 | 0 | 100000 | 1 | yes | 0.202 | 0.787 | -0.2 | 0.08 | 0.4 |  |
| uring | 1 | 0 | 200000 | 1 | yes | 1.244 | 4.661 | -1.1 | 0.04 | 0.5 |  |
| uring | 1 | 0 | 400000 | 1 | yes | 3.023 | 22.446 | -5.2 | 0.03 | 0.6 |  |
| uring | 1 | 0 | 600000 | 1 | yes | 19.087 | 868.221 | 136.9 | 0.00 | 0.7 |  |
| uring | 1 | 0 | 800000 | 1 | no | 3131.048 | 6836.716 | 3173.4 | 0.67 | 0.6 | missed slots, lateness ratio |
| mosquitto | 1 | 1 | 25000 | 1 | yes | 0.297 | 0.672 | -0.4 | 0.10 | 0.9 |  |
| mosquitto | 1 | 1 | 50000 | 1 | yes | 0.375 | 1.093 | -0.4 | 0.06 | 0.5 |  |
| mosquitto | 1 | 1 | 75000 | 1 | yes | 0.852 | 238.289 | -2.0 | 0.00 | 0.4 |  |
| mosquitto | 1 | 1 | 100000 | 1 | no | 8396.997 | 13992.198 | 12573.1 | 0.00 | 0.6 | unresolved, stalled, unacknowledged |
| tokio | 1 | 1 | 50000 | 1 | yes | 0.174 | 0.504 | -0.1 | 0.08 | 0.2 |  |
| tokio | 1 | 1 | 100000 | 1 | yes | 0.350 | 2.017 | -0.3 | 0.04 | 0.3 |  |
| tokio | 1 | 1 | 200000 | 1 | yes | 1.884 | 5.738 | -2.1 | 0.03 | 0.5 |  |
| tokio | 1 | 1 | 300000 | 1 | yes | 3.877 | 108.528 | -6.3 | 0.00 | 0.5 |  |
| tokio | 1 | 1 | 400000 | 1 | no | 5406.458 | 23152.558 | 12701.7 | 0.02 | 0.7 | missed slots |
| tokio | 1 | 1 | 500000 | 1 | no | 3980.394 | 30282.875 | 19691.5 | 0.61 | 0.6 | missed slots, lateness ratio |
| uring | 1 | 1 | 50000 | 1 | yes | 0.141 | 0.488 | -0.1 | 0.09 | 0.3 |  |
| uring | 1 | 1 | 100000 | 1 | yes | 0.305 | 1.466 | -0.5 | 0.06 | 0.4 |  |
| uring | 1 | 1 | 200000 | 1 | yes | 1.606 | 5.358 | -2.0 | 0.03 | 0.6 |  |
| uring | 1 | 1 | 300000 | 1 | yes | 3.088 | 16.712 | -4.3 | 0.02 | 0.6 |  |
| uring | 1 | 1 | 400000 | 1 | yes | 7.901 | 580.911 | -4.5 | 0.00 | 0.7 |  |
| uring | 1 | 1 | 500000 | 1 | no | 865.599 | 18421.383 | 4086.0 | 0.29 | 0.7 | missed slots, lateness ratio |
| tokio | 4 | 0 | 250000 | 1 | no | 0.705 | 4.477 | -0.9 | 0.19 | 3.2 | lateness ratio |
| tokio | 4 | 0 | 500000 | 1 | no | 1.229 | 8.872 | -3.6 | 0.20 | 3.2 | lateness ratio |
| tokio | 4 | 0 | 1000000 | 1 | yes | 2.236 | 35.946 | -9.0 | 0.05 | 3.1 |  |
| tokio | 4 | 0 | 1500000 | 1 | yes | 3.011 | 126.419 | -16.6 | 0.02 | 2.6 |  |
| tokio | 4 | 0 | 2000000 | 1 | no | 6.595 | 1325.400 | -11.1 | 0.00 | 2.2 | missed slots |
| uring | 4 | 0 | 250000 | 1 | no | 0.738 | 4.506 | -0.8 | 0.15 | 2.8 | lateness ratio |
| uring | 4 | 0 | 500000 | 1 | no | 1.316 | 6.513 | -1.0 | 0.22 | 2.8 | lateness ratio |
| uring | 4 | 0 | 1000000 | 1 | no | 2.099 | 13.320 | -2.1 | 0.15 | 2.6 | lateness ratio |
| uring | 4 | 0 | 1500000 | 1 | yes | 4.444 | 219.415 | -4.4 | 0.02 | 3.1 |  |
| uring | 4 | 0 | 2000000 | 1 | no | 7.139 | 2109.735 | 377.7 | 0.75 | 2.0 | missed slots, lateness ratio |
| tokio | 4 | 1 | 250000 | 1 | no | 1.010 | 5.911 | -1.3 | 0.18 | 2.8 | lateness ratio |
| tokio | 4 | 1 | 500000 | 1 | no | 1.727 | 13.779 | -3.1 | 0.13 | 3.0 | lateness ratio |
| tokio | 4 | 1 | 750000 | 1 | yes | 2.621 | 166.330 | -4.0 | 0.02 | 3.9 |  |
| tokio | 4 | 1 | 1000000 | 1 | yes | 2.859 | 72.810 | 8.0 | 0.03 | 2.8 |  |
| tokio | 4 | 1 | 1250000 | 1 | yes | 5.677 | 2483.028 | 544.7 | 0.00 | 2.7 |  |
| uring | 4 | 1 | 250000 | 1 | no | 0.922 | 5.505 | -0.9 | 0.23 | 2.8 | lateness ratio |
| uring | 4 | 1 | 500000 | 1 | no | 1.588 | 10.404 | -2.1 | 0.16 | 3.1 | lateness ratio |
| uring | 4 | 1 | 750000 | 1 | no | 2.077 | 14.819 | -2.5 | 0.15 | 2.9 | lateness ratio |
| uring | 4 | 1 | 1000000 | 1 | yes | 2.404 | 31.916 | -1.5 | 0.06 | 2.8 |  |
| uring | 4 | 1 | 1250000 | 1 | yes | 5.689 | 694.157 | 9.1 | 0.01 | 2.4 |  |

### Fine ladder at 4 workers (release, 2 repetitions)

| Server | Workers | QoS | Rate (msgs/s) | Rep | Valid | p50 (ms) | p99 (ms) | growth (ms) | lateness ratio | pressure (%) | failed rows |
|---|---|---|---|---|---|---|---|---|---|---|---|
| tokio | 4 | 0 | 1200000 | 1 | yes | 2.431 | 37.454 | -4.2 | 0.06 | 2.7 |  |
| tokio | 4 | 0 | 1200000 | 2 | yes | 2.267 | 32.899 | -2.3 | 0.07 | 2.7 |  |
| tokio | 4 | 0 | 1300000 | 1 | yes | 2.771 | 65.208 | -5.2 | 0.04 | 2.9 |  |
| tokio | 4 | 0 | 1300000 | 2 | yes | 2.750 | 71.107 | -10.1 | 0.03 | 3.2 |  |
| tokio | 4 | 0 | 1500000 | 1 | yes | 3.166 | 113.967 | -7.3 | 0.02 | 2.6 |  |
| tokio | 4 | 0 | 1500000 | 2 | yes | 3.148 | 154.796 | -36.0 | 0.02 | 3.5 |  |
| tokio | 4 | 0 | 1700000 | 1 | yes | 3.582 | 135.791 | -4.2 | 0.02 | 2.5 |  |
| tokio | 4 | 0 | 1700000 | 2 | yes | 4.108 | 781.189 | -17.4 | 0.01 | 3.9 |  |
| uring | 4 | 0 | 1200000 | 1 | yes | 2.429 | 29.606 | -1.2 | 0.07 | 2.7 |  |
| uring | 4 | 0 | 1200000 | 2 | no | 2.507 | 17.842 | -3.1 | 0.11 | 2.7 | lateness ratio |
| uring | 4 | 0 | 1300000 | 1 | yes | 3.060 | 87.949 | -8.1 | 0.02 | 2.5 |  |
| uring | 4 | 0 | 1300000 | 2 | yes | 3.041 | 224.133 | -7.4 | 0.01 | 2.9 |  |
| uring | 4 | 0 | 1500000 | 1 | no | 3.471 | 399.507 | 41.4 | 0.01 | 2.8 | missed slots |
| uring | 4 | 0 | 1500000 | 2 | yes | 3.162 | 86.245 | -5.5 | 0.03 | 2.8 |  |
| uring | 4 | 0 | 1700000 | 1 | no | 5.321 | 7532.970 | -158.1 | 0.82 | 2.3 | lateness ratio |
| uring | 4 | 0 | 1700000 | 2 | no | 6.070 | 1377.829 | 343.8 | 0.36 | 2.3 | missed slots, lateness ratio |
| tokio | 4 | 1 | 900000 | 1 | yes | 2.740 | 36.372 | -4.5 | 0.07 | 2.9 |  |
| tokio | 4 | 1 | 900000 | 2 | yes | 2.716 | 59.376 | -3.9 | 0.04 | 3.1 |  |
| tokio | 4 | 1 | 1000000 | 1 | yes | 3.219 | 206.963 | -2.5 | 0.01 | 3.0 |  |
| tokio | 4 | 1 | 1000000 | 2 | yes | 3.189 | 269.222 | -5.0 | 0.01 | 3.2 |  |
| tokio | 4 | 1 | 1100000 | 1 | yes | 3.623 | 150.733 | -4.1 | 0.02 | 2.8 |  |
| tokio | 4 | 1 | 1100000 | 2 | yes | 3.025 | 81.854 | -9.5 | 0.03 | 2.9 |  |
| tokio | 4 | 1 | 1200000 | 1 | yes | 3.863 | 197.263 | 6.5 | 0.01 | 2.6 |  |
| tokio | 4 | 1 | 1200000 | 2 | yes | 4.583 | 912.785 | -4.4 | 0.00 | 3.2 |  |
| uring | 4 | 1 | 900000 | 1 | yes | 2.226 | 52.003 | -2.3 | 0.04 | 2.8 |  |
| uring | 4 | 1 | 900000 | 2 | yes | 2.298 | 29.393 | -3.5 | 0.07 | 2.9 |  |
| uring | 4 | 1 | 1000000 | 1 | yes | 2.734 | 38.175 | -2.0 | 0.06 | 2.6 |  |
| uring | 4 | 1 | 1000000 | 2 | yes | 2.550 | 42.893 | -5.0 | 0.06 | 2.9 |  |
| uring | 4 | 1 | 1100000 | 1 | yes | 3.908 | 922.747 | -2.7 | 0.00 | 2.6 |  |
| uring | 4 | 1 | 1100000 | 2 | yes | 3.473 | 62.259 | -8.8 | 0.04 | 2.7 |  |
| uring | 4 | 1 | 1200000 | 1 | yes | 3.574 | 90.898 | -9.3 | 0.03 | 2.5 |  |
| uring | 4 | 1 | 1200000 | 2 | yes | 4.747 | 241.959 | -13.3 | 0.01 | 2.6 |  |

### Confirmation at 1 worker (release, 5 repetitions)

| Server | Workers | QoS | Rate (msgs/s) | Rep | Valid | p50 (ms) | p99 (ms) | growth (ms) | lateness ratio | pressure (%) | failed rows |
|---|---|---|---|---|---|---|---|---|---|---|---|
| mosquitto | 1 | 0 | 100000 | 1 | yes | 0.691 | 4.616 | -0.5 | 0.02 | 0.5 |  |
| mosquitto | 1 | 0 | 100000 | 2 | yes | 0.778 | 4.678 | -0.8 | 0.03 | 0.5 |  |
| mosquitto | 1 | 0 | 100000 | 3 | yes | 0.725 | 4.567 | -0.5 | 0.02 | 0.5 |  |
| mosquitto | 1 | 0 | 100000 | 4 | yes | 0.593 | 2.744 | -0.5 | 0.03 | 0.6 |  |
| mosquitto | 1 | 0 | 100000 | 5 | yes | 0.671 | 2.990 | -0.7 | 0.03 | 0.5 |  |
| mosquitto | 1 | 0 | 150000 | 1 | yes | 1.681 | 19.104 | -2.8 | 0.01 | 0.5 |  |
| mosquitto | 1 | 0 | 150000 | 2 | yes | 1.724 | 25.084 | -1.0 | 0.00 | 0.4 |  |
| mosquitto | 1 | 0 | 150000 | 3 | yes | 1.842 | 44.270 | -0.9 | 0.00 | 0.5 |  |
| mosquitto | 1 | 0 | 150000 | 4 | yes | 1.761 | 29.032 | -1.5 | 0.00 | 0.4 |  |
| mosquitto | 1 | 0 | 150000 | 5 | yes | 1.633 | 33.358 | -11.8 | 0.00 | 0.5 |  |
| tokio | 1 | 0 | 150000 | 1 | yes | 0.738 | 4.184 | -0.8 | 0.03 | 0.4 |  |
| tokio | 1 | 0 | 150000 | 2 | yes | 0.783 | 3.987 | -0.7 | 0.04 | 0.4 |  |
| tokio | 1 | 0 | 150000 | 3 | yes | 0.721 | 3.848 | -1.1 | 0.03 | 0.4 |  |
| tokio | 1 | 0 | 150000 | 4 | yes | 0.795 | 4.395 | -1.0 | 0.03 | 0.3 |  |
| tokio | 1 | 0 | 150000 | 5 | yes | 0.777 | 3.647 | -0.8 | 0.04 | 0.4 |  |
| tokio | 1 | 0 | 200000 | 1 | yes | 1.067 | 5.267 | -0.9 | 0.04 | 0.5 |  |
| tokio | 1 | 0 | 200000 | 2 | yes | 1.018 | 4.604 | -1.5 | 0.04 | 0.4 |  |
| tokio | 1 | 0 | 200000 | 3 | yes | 0.971 | 4.309 | -1.4 | 0.05 | 0.4 |  |
| tokio | 1 | 0 | 200000 | 4 | yes | 1.059 | 4.592 | -1.5 | 0.04 | 0.4 |  |
| tokio | 1 | 0 | 200000 | 5 | yes | 1.025 | 4.538 | -1.3 | 0.04 | 0.4 |  |
| tokio | 1 | 0 | 400000 | 1 | yes | 2.869 | 60.686 | -11.2 | 0.01 | 0.5 |  |
| tokio | 1 | 0 | 400000 | 2 | yes | 3.103 | 85.393 | -10.6 | 0.01 | 0.5 |  |
| tokio | 1 | 0 | 400000 | 3 | yes | 2.363 | 47.120 | -4.4 | 0.01 | 0.5 |  |
| tokio | 1 | 0 | 400000 | 4 | yes | 2.937 | 120.979 | -1.6 | 0.00 | 0.6 |  |
| tokio | 1 | 0 | 400000 | 5 | yes | 2.830 | 26.640 | -5.6 | 0.02 | 0.6 |  |
| uring | 1 | 0 | 150000 | 1 | yes | 0.792 | 4.133 | -0.4 | 0.04 | 0.4 |  |
| uring | 1 | 0 | 150000 | 2 | yes | 0.748 | 4.317 | -1.1 | 0.04 | 0.5 |  |
| uring | 1 | 0 | 150000 | 3 | yes | 0.779 | 4.309 | -1.2 | 0.04 | 0.4 |  |
| uring | 1 | 0 | 150000 | 4 | yes | 0.745 | 4.094 | -0.5 | 0.04 | 0.5 |  |
| uring | 1 | 0 | 150000 | 5 | yes | 0.761 | 4.151 | -1.0 | 0.04 | 0.4 |  |
| uring | 1 | 0 | 200000 | 1 | yes | 1.150 | 4.956 | -1.3 | 0.05 | 0.5 |  |
| uring | 1 | 0 | 200000 | 2 | yes | 1.158 | 4.547 | -2.4 | 0.05 | 0.5 |  |
| uring | 1 | 0 | 200000 | 3 | yes | 1.232 | 4.522 | -0.9 | 0.05 | 0.5 |  |
| uring | 1 | 0 | 200000 | 4 | yes | 1.146 | 4.530 | -2.1 | 0.06 | 0.6 |  |
| uring | 1 | 0 | 200000 | 5 | yes | 1.129 | 4.399 | -1.2 | 0.06 | 0.5 |  |
| uring | 1 | 0 | 400000 | 1 | yes | 3.361 | 15.557 | -1.5 | 0.04 | 0.7 |  |
| uring | 1 | 0 | 400000 | 2 | yes | 2.953 | 15.507 | -1.3 | 0.05 | 0.7 |  |
| uring | 1 | 0 | 400000 | 3 | yes | 3.191 | 14.934 | -1.4 | 0.04 | 0.6 |  |
| uring | 1 | 0 | 400000 | 4 | yes | 3.873 | 42.041 | -2.1 | 0.01 | 0.7 |  |
| uring | 1 | 0 | 400000 | 5 | yes | 2.863 | 19.235 | -1.5 | 0.04 | 0.7 |  |
| mosquitto | 1 | 1 | 50000 | 1 | yes | 0.353 | 2.130 | -0.3 | 0.03 | 0.7 |  |
| mosquitto | 1 | 1 | 50000 | 2 | yes | 0.374 | 1.309 | -0.4 | 0.05 | 0.7 |  |
| mosquitto | 1 | 1 | 50000 | 3 | yes | 0.382 | 1.615 | -0.7 | 0.04 | 0.6 |  |
| mosquitto | 1 | 1 | 50000 | 4 | yes | 0.362 | 1.202 | -0.5 | 0.06 | 0.7 |  |
| mosquitto | 1 | 1 | 50000 | 5 | yes | 0.407 | 1.324 | -0.4 | 0.04 | 0.5 |  |
| mosquitto | 1 | 1 | 75000 | 1 | yes | 0.689 | 36.307 | -0.5 | 0.00 | 0.6 |  |
| mosquitto | 1 | 1 | 75000 | 2 | yes | 0.630 | 47.645 | -10.6 | 0.00 | 0.6 |  |
| mosquitto | 1 | 1 | 75000 | 3 | yes | 0.590 | 26.886 | -0.6 | 0.00 | 0.7 |  |
| mosquitto | 1 | 1 | 75000 | 4 | yes | 0.763 | 70.255 | -1.6 | 0.00 | 0.6 |  |
| mosquitto | 1 | 1 | 75000 | 5 | yes | 0.547 | 45.056 | -0.3 | 0.00 | 0.6 |  |
| tokio | 1 | 1 | 75000 | 1 | yes | 0.211 | 0.724 | -0.3 | 0.08 | 0.3 |  |
| tokio | 1 | 1 | 75000 | 2 | yes | 0.217 | 1.407 | -0.2 | 0.05 | 0.3 |  |
| tokio | 1 | 1 | 75000 | 3 | yes | 0.208 | 0.707 | -0.8 | 0.09 | 0.3 |  |
| tokio | 1 | 1 | 75000 | 4 | yes | 0.213 | 1.046 | -0.1 | 0.06 | 0.3 |  |
| tokio | 1 | 1 | 75000 | 5 | yes | 0.215 | 0.808 | -0.3 | 0.08 | 0.3 |  |
| tokio | 1 | 1 | 200000 | 1 | yes | 1.766 | 5.431 | -1.2 | 0.04 | 0.5 |  |
| tokio | 1 | 1 | 200000 | 2 | yes | 1.872 | 6.509 | -1.4 | 0.03 | 0.4 |  |
| tokio | 1 | 1 | 200000 | 3 | yes | 1.862 | 6.271 | -0.8 | 0.03 | 0.4 |  |
| tokio | 1 | 1 | 200000 | 4 | yes | 1.862 | 6.001 | -2.5 | 0.04 | 0.5 |  |
| tokio | 1 | 1 | 200000 | 5 | yes | 1.924 | 6.148 | -2.2 | 0.03 | 0.4 |  |
| tokio | 1 | 1 | 300000 | 1 | yes | 4.014 | 136.970 | -4.6 | 0.00 | 0.5 |  |
| tokio | 1 | 1 | 300000 | 2 | yes | 3.654 | 113.246 | -1.7 | 0.00 | 0.5 |  |
| tokio | 1 | 1 | 300000 | 3 | yes | 3.936 | 179.044 | -8.7 | 0.00 | 0.5 |  |
| tokio | 1 | 1 | 300000 | 4 | yes | 3.881 | 191.234 | -4.6 | 0.00 | 0.5 |  |
| tokio | 1 | 1 | 300000 | 5 | yes | 3.523 | 100.270 | -1.9 | 0.00 | 0.5 |  |
| uring | 1 | 1 | 75000 | 1 | yes | 0.171 | 0.603 | -0.2 | 0.09 | 0.4 |  |
| uring | 1 | 1 | 75000 | 2 | yes | 0.161 | 0.590 | -0.2 | 0.10 | 0.3 |  |
| uring | 1 | 1 | 75000 | 3 | yes | 0.168 | 0.754 | -0.4 | 0.08 | 0.4 |  |
| uring | 1 | 1 | 75000 | 4 | yes | 0.176 | 0.661 | -0.2 | 0.09 | 0.3 |  |
| uring | 1 | 1 | 75000 | 5 | yes | 0.168 | 0.610 | -0.1 | 0.09 | 0.3 |  |
| uring | 1 | 1 | 200000 | 1 | yes | 1.435 | 5.247 | -0.7 | 0.04 | 0.6 |  |
| uring | 1 | 1 | 200000 | 2 | yes | 1.460 | 5.128 | -1.1 | 0.04 | 0.6 |  |
| uring | 1 | 1 | 200000 | 3 | yes | 1.370 | 5.489 | -1.0 | 0.04 | 0.5 |  |
| uring | 1 | 1 | 200000 | 4 | yes | 1.463 | 5.206 | -1.9 | 0.04 | 0.6 |  |
| uring | 1 | 1 | 200000 | 5 | yes | 1.559 | 13.443 | -2.3 | 0.07 | 0.6 |  |
| uring | 1 | 1 | 300000 | 1 | yes | 3.357 | 16.572 | -3.1 | 0.02 | 0.6 |  |
| uring | 1 | 1 | 300000 | 2 | yes | 3.402 | 14.262 | -4.9 | 0.03 | 0.6 |  |
| uring | 1 | 1 | 300000 | 3 | yes | 3.463 | 19.513 | -3.3 | 0.02 | 0.6 |  |
| uring | 1 | 1 | 300000 | 4 | yes | 3.766 | 30.245 | -1.9 | 0.01 | 0.6 |  |
| uring | 1 | 1 | 300000 | 5 | yes | 3.312 | 13.500 | -2.7 | 0.03 | 0.6 |  |

### Smoke ladder (debug build, smoke timing, 5 repetitions)

| Server | Workers | QoS | Rate (msgs/s) | Rep | Valid | p50 (ms) | p99 (ms) | growth (ms) | lateness ratio | pressure (%) | failed rows |
|---|---|---|---|---|---|---|---|---|---|---|---|
| tokio | 1 | 0 | 1000 | 1 | no | 0.155 | 0.315 | 0.0 | 0.23 | 0.8 | lateness ratio |
| tokio | 1 | 0 | 1000 | 2 | no | 0.148 | 0.301 | 0.0 | 0.23 | 0.7 | lateness ratio |
| tokio | 1 | 0 | 1000 | 3 | no | 0.146 | 0.305 | 0.0 | 0.25 | 0.7 | lateness ratio |
| tokio | 1 | 0 | 1000 | 4 | no | 0.143 | 0.299 | 0.0 | 0.23 | 0.6 | lateness ratio |
| tokio | 1 | 0 | 1000 | 5 | no | 0.142 | 0.274 | 0.0 | 0.25 | 0.7 | lateness ratio |
| tokio | 1 | 0 | 2000 | 1 | no | 0.136 | 0.278 | 0.0 | 0.22 | 0.6 | lateness ratio |
| tokio | 1 | 0 | 2000 | 2 | no | 0.136 | 0.266 | 0.0 | 0.23 | 0.5 | lateness ratio |
| tokio | 1 | 0 | 2000 | 3 | no | 0.151 | 0.325 | 0.0 | 0.23 | 0.5 | lateness ratio |
| tokio | 1 | 0 | 2000 | 4 | no | 0.138 | 0.287 | 0.0 | 0.21 | 0.5 | lateness ratio |
| tokio | 1 | 0 | 2000 | 5 | no | 0.132 | 0.232 | 0.0 | 0.25 | 0.5 | lateness ratio |
| tokio | 1 | 0 | 5000 | 1 | no | 0.134 | 0.313 | 0.0 | 0.15 | 0.3 | lateness ratio |
| tokio | 1 | 0 | 5000 | 2 | no | 0.134 | 0.324 | 0.0 | 0.15 | 0.3 | lateness ratio |
| tokio | 1 | 0 | 5000 | 3 | no | 0.123 | 0.297 | 0.0 | 0.15 | 0.3 | lateness ratio |
| tokio | 1 | 0 | 5000 | 4 | no | 0.126 | 0.329 | 0.0 | 0.14 | 0.3 | lateness ratio |
| tokio | 1 | 0 | 5000 | 5 | no | 0.129 | 0.338 | -0.2 | 0.14 | 0.3 | lateness ratio |
| tokio | 1 | 0 | 10000 | 1 | no | 0.173 | 0.400 | -0.2 | 0.12 | 0.4 | lateness ratio |
| tokio | 1 | 0 | 10000 | 2 | no | 0.174 | 0.417 | -0.1 | 0.12 | 0.4 | lateness ratio |
| tokio | 1 | 0 | 10000 | 3 | no | 0.169 | 0.381 | -0.2 | 0.12 | 0.4 | lateness ratio |
| tokio | 1 | 0 | 10000 | 4 | no | 0.168 | 0.386 | -0.2 | 0.13 | 0.4 | lateness ratio |
| tokio | 1 | 0 | 10000 | 5 | no | 0.168 | 0.388 | -0.1 | 0.12 | 0.4 | lateness ratio |
| tokio | 1 | 0 | 20000 | 1 | yes | 0.202 | 0.571 | -0.1 | 0.09 | 0.3 |  |
| tokio | 1 | 0 | 20000 | 2 | no | 0.235 | 0.557 | -0.1 | 0.10 | 0.2 | lateness ratio |
| tokio | 1 | 0 | 20000 | 3 | no | 0.203 | 0.487 | -0.1 | 0.12 | 0.2 | lateness ratio |
| tokio | 1 | 0 | 20000 | 4 | yes | 0.224 | 0.694 | -0.2 | 0.08 | 0.3 |  |
| tokio | 1 | 0 | 20000 | 5 | no | 0.212 | 0.551 | -0.1 | 0.10 | 0.2 | lateness ratio |
| uring | 1 | 0 | 1000 | 1 | no | 0.144 | 0.304 | 0.0 | 0.23 | 0.6 | lateness ratio |
| uring | 1 | 0 | 1000 | 2 | no | 0.139 | 0.294 | 0.0 | 0.23 | 0.6 | lateness ratio |
| uring | 1 | 0 | 1000 | 3 | no | 0.137 | 0.276 | 0.0 | 0.25 | 0.6 | lateness ratio |
| uring | 1 | 0 | 1000 | 4 | no | 0.138 | 0.279 | 0.0 | 0.26 | 0.6 | lateness ratio |
| uring | 1 | 0 | 1000 | 5 | no | 0.144 | 0.300 | 0.0 | 0.22 | 0.4 | lateness ratio |
| uring | 1 | 0 | 2000 | 1 | no | 0.133 | 0.276 | 0.0 | 0.23 | 0.5 | lateness ratio |
| uring | 1 | 0 | 2000 | 2 | no | 0.128 | 0.249 | 0.0 | 0.23 | 0.4 | lateness ratio |
| uring | 1 | 0 | 2000 | 3 | no | 0.129 | 0.259 | 0.0 | 0.23 | 0.5 | lateness ratio |
| uring | 1 | 0 | 2000 | 4 | no | 0.127 | 0.262 | 0.0 | 0.22 | 0.5 | lateness ratio |
| uring | 1 | 0 | 2000 | 5 | no | 0.124 | 0.214 | 0.0 | 0.28 | 0.4 | lateness ratio |
| uring | 1 | 0 | 5000 | 1 | no | 0.118 | 0.315 | 0.0 | 0.14 | 0.3 | lateness ratio |
| uring | 1 | 0 | 5000 | 2 | no | 0.126 | 0.328 | 0.0 | 0.14 | 0.3 | lateness ratio |
| uring | 1 | 0 | 5000 | 3 | no | 0.119 | 0.301 | 0.0 | 0.16 | 0.3 | lateness ratio |
| uring | 1 | 0 | 5000 | 4 | no | 0.123 | 0.328 | 0.0 | 0.14 | 0.3 | lateness ratio |
| uring | 1 | 0 | 5000 | 5 | no | 0.119 | 0.298 | 0.0 | 0.15 | 0.3 | lateness ratio |
| uring | 1 | 0 | 10000 | 1 | no | 0.157 | 0.374 | -0.1 | 0.13 | 0.4 | lateness ratio |
| uring | 1 | 0 | 10000 | 2 | no | 0.157 | 0.397 | -0.1 | 0.12 | 0.4 | lateness ratio |
| uring | 1 | 0 | 10000 | 3 | no | 0.156 | 0.381 | -0.1 | 0.13 | 0.4 | lateness ratio |
| uring | 1 | 0 | 10000 | 4 | no | 0.170 | 0.477 | -0.2 | 0.11 | 0.5 | lateness ratio |
| uring | 1 | 0 | 10000 | 5 | no | 0.162 | 0.382 | -0.1 | 0.13 | 0.4 | lateness ratio |
| uring | 1 | 0 | 20000 | 1 | no | 0.186 | 0.462 | -0.2 | 0.12 | 0.3 | lateness ratio |
| uring | 1 | 0 | 20000 | 2 | no | 0.193 | 0.459 | -0.1 | 0.13 | 0.3 | lateness ratio |
| uring | 1 | 0 | 20000 | 3 | no | 0.188 | 0.471 | -0.1 | 0.12 | 0.4 | lateness ratio |
| uring | 1 | 0 | 20000 | 4 | no | 0.182 | 0.466 | -0.2 | 0.12 | 0.3 | lateness ratio |
| uring | 1 | 0 | 20000 | 5 | no | 0.200 | 0.505 | -0.1 | 0.11 | 0.3 | lateness ratio |
| mosquitto | 1 | 1 | 1000 | 1 | no | 0.205 | 0.349 | 0.0 | 0.20 | 0.8 | lateness ratio |
| mosquitto | 1 | 1 | 1000 | 2 | no | 0.202 | 0.375 | 0.0 | 0.19 | 0.6 | lateness ratio |
| mosquitto | 1 | 1 | 1000 | 3 | no | 0.219 | 0.388 | 0.0 | 0.19 | 0.7 | lateness ratio |
| mosquitto | 1 | 1 | 1000 | 4 | no | 0.203 | 0.357 | 0.0 | 0.19 | 0.7 | lateness ratio |
| mosquitto | 1 | 1 | 1000 | 5 | no | 0.218 | 0.367 | 0.0 | 0.18 | 0.9 | lateness ratio |
| mosquitto | 1 | 1 | 2000 | 1 | no | 0.194 | 0.334 | 0.0 | 0.19 | 0.5 | lateness ratio |
| mosquitto | 1 | 1 | 2000 | 2 | no | 0.197 | 0.359 | 0.0 | 0.16 | 0.5 | lateness ratio |
| mosquitto | 1 | 1 | 2000 | 3 | no | 0.191 | 0.416 | 0.0 | 0.14 | 0.6 | lateness ratio |
| mosquitto | 1 | 1 | 2000 | 4 | no | 0.191 | 0.353 | 0.0 | 0.16 | 0.5 | lateness ratio |
| mosquitto | 1 | 1 | 2000 | 5 | no | 0.199 | 0.345 | 0.0 | 0.16 | 0.5 | lateness ratio |
| mosquitto | 1 | 1 | 5000 | 1 | no | 0.205 | 0.392 | -0.2 | 0.13 | 0.4 | lateness ratio |
| mosquitto | 1 | 1 | 5000 | 2 | no | 0.210 | 0.393 | 0.0 | 0.13 | 0.3 | lateness ratio |
| mosquitto | 1 | 1 | 5000 | 3 | no | 0.201 | 0.385 | 0.0 | 0.13 | 0.4 | lateness ratio |
| mosquitto | 1 | 1 | 5000 | 4 | no | 0.211 | 0.391 | -0.2 | 0.13 | 0.4 | lateness ratio |
| mosquitto | 1 | 1 | 5000 | 5 | no | 0.232 | 0.427 | 0.0 | 0.12 | 0.4 | lateness ratio |
| mosquitto | 1 | 1 | 10000 | 1 | no | 0.252 | 0.529 | -0.2 | 0.11 | 0.8 | lateness ratio |
| mosquitto | 1 | 1 | 10000 | 2 | no | 0.250 | 0.519 | -0.2 | 0.11 | 0.8 | lateness ratio |
| mosquitto | 1 | 1 | 10000 | 3 | no | 0.266 | 0.516 | -0.3 | 0.10 | 0.6 | lateness ratio |
| mosquitto | 1 | 1 | 10000 | 4 | no | 0.269 | 0.525 | -0.2 | 0.10 | 0.8 | lateness ratio |
| mosquitto | 1 | 1 | 10000 | 5 | no | 0.242 | 0.437 | -0.2 | 0.12 | 0.6 | lateness ratio |
| mosquitto | 1 | 1 | 20000 | 1 | no | 0.334 | 0.706 | -0.2 | 0.10 | 0.6 | lateness ratio |
| mosquitto | 1 | 1 | 20000 | 2 | yes | 0.353 | 1.094 | -0.3 | 0.07 | 0.6 |  |
| mosquitto | 1 | 1 | 20000 | 3 | yes | 0.275 | 0.843 | -0.2 | 0.09 | 0.6 |  |
| mosquitto | 1 | 1 | 20000 | 4 | no | 0.299 | 0.672 | -0.2 | 0.11 | 0.5 | lateness ratio |
| mosquitto | 1 | 1 | 20000 | 5 | no | 0.307 | 0.657 | -0.2 | 0.13 | 0.7 | lateness ratio |
| tokio | 1 | 1 | 1000 | 1 | no | 0.164 | 0.315 | 0.0 | 0.24 | 0.7 | lateness ratio |
| tokio | 1 | 1 | 1000 | 2 | no | 0.159 | 0.301 | 0.0 | 0.25 | 0.6 | lateness ratio |
| tokio | 1 | 1 | 1000 | 3 | no | 0.147 | 0.302 | 0.0 | 0.23 | 0.6 | lateness ratio |
| tokio | 1 | 1 | 1000 | 4 | no | 0.146 | 0.274 | 0.0 | 0.25 | 0.5 | lateness ratio |
| tokio | 1 | 1 | 1000 | 5 | no | 0.151 | 0.311 | 0.0 | 0.23 | 0.6 | lateness ratio |
| tokio | 1 | 1 | 2000 | 1 | no | 0.156 | 0.289 | 0.0 | 0.22 | 0.5 | lateness ratio |
| tokio | 1 | 1 | 2000 | 2 | no | 0.147 | 0.246 | 0.0 | 0.24 | 0.5 | lateness ratio |
| tokio | 1 | 1 | 2000 | 3 | no | 0.143 | 0.262 | 0.0 | 0.22 | 0.5 | lateness ratio |
| tokio | 1 | 1 | 2000 | 4 | no | 0.140 | 0.239 | 0.0 | 0.25 | 0.5 | lateness ratio |
| tokio | 1 | 1 | 2000 | 5 | no | 0.140 | 0.233 | 0.0 | 0.27 | 0.5 | lateness ratio |
| tokio | 1 | 1 | 5000 | 1 | no | 0.184 | 0.353 | 0.0 | 0.17 | 0.3 | lateness ratio |
| tokio | 1 | 1 | 5000 | 2 | no | 0.170 | 0.363 | 0.0 | 0.14 | 0.3 | lateness ratio |
| tokio | 1 | 1 | 5000 | 3 | no | 0.147 | 0.314 | -0.2 | 0.15 | 0.3 | lateness ratio |
| tokio | 1 | 1 | 5000 | 4 | no | 0.146 | 0.338 | 0.0 | 0.15 | 0.3 | lateness ratio |
| tokio | 1 | 1 | 5000 | 5 | no | 0.144 | 0.334 | 0.0 | 0.15 | 0.3 | lateness ratio |
| tokio | 1 | 1 | 10000 | 1 | no | 0.221 | 0.476 | -0.1 | 0.13 | 0.5 | lateness ratio |
| tokio | 1 | 1 | 10000 | 2 | no | 0.223 | 0.525 | -0.2 | 0.11 | 0.5 | lateness ratio |
| tokio | 1 | 1 | 10000 | 3 | no | 0.206 | 0.443 | -0.1 | 0.12 | 0.5 | lateness ratio |
| tokio | 1 | 1 | 10000 | 4 | no | 0.214 | 0.507 | -0.2 | 0.11 | 0.5 | lateness ratio |
| tokio | 1 | 1 | 10000 | 5 | no | 0.207 | 0.450 | -0.2 | 0.13 | 0.4 | lateness ratio |
| tokio | 1 | 1 | 20000 | 1 | yes | 0.348 | 0.834 | -0.3 | 0.08 | 0.3 |  |
| tokio | 1 | 1 | 20000 | 1 | yes | 0.407 | 1.873 | -0.2 | 0.04 | 0.4 |  |
| tokio | 1 | 1 | 20000 | 2 | yes | 0.366 | 0.964 | -0.3 | 0.07 | 0.3 |  |
| tokio | 1 | 1 | 20000 | 2 | yes | 0.306 | 0.914 | -0.2 | 0.07 | 0.4 |  |
| tokio | 1 | 1 | 20000 | 3 | yes | 0.333 | 0.930 | -0.2 | 0.06 | 0.3 |  |
| tokio | 1 | 1 | 20000 | 3 | yes | 0.360 | 1.178 | -0.5 | 0.07 | 0.3 |  |
| tokio | 1 | 1 | 20000 | 4 | yes | 0.313 | 0.793 | -0.3 | 0.08 | 0.4 |  |
| tokio | 1 | 1 | 20000 | 4 | yes | 0.455 | 1.510 | -0.4 | 0.05 | 0.3 |  |
| tokio | 1 | 1 | 20000 | 5 | yes | 0.354 | 0.760 | -0.3 | 0.08 | 0.3 |  |
| tokio | 1 | 1 | 20000 | 5 | yes | 0.409 | 2.039 | -0.3 | 0.04 | 0.4 |  |
| tokio | 1 | 1 | 30000 | 1 | yes | 1.038 | 6.246 | -1.3 | 0.02 | 0.4 |  |
| tokio | 1 | 1 | 30000 | 2 | yes | 0.949 | 5.427 | -0.2 | 0.02 | 0.3 |  |
| tokio | 1 | 1 | 30000 | 3 | yes | 1.019 | 6.443 | -0.3 | 0.02 | 0.4 |  |
| tokio | 1 | 1 | 30000 | 4 | yes | 1.014 | 5.800 | -2.7 | 0.02 | 0.3 |  |
| tokio | 1 | 1 | 30000 | 5 | yes | 1.149 | 6.734 | -0.5 | 0.01 | 0.3 |  |
| tokio | 1 | 1 | 40000 | 1 | yes | 1.080 | 7.926 | -0.5 | 0.02 | 0.3 |  |
| tokio | 1 | 1 | 40000 | 2 | yes | 1.391 | 8.196 | -0.2 | 0.02 | 0.4 |  |
| tokio | 1 | 1 | 40000 | 3 | yes | 1.290 | 8.106 | -1.9 | 0.02 | 0.4 |  |
| tokio | 1 | 1 | 40000 | 4 | yes | 1.398 | 8.708 | -0.7 | 0.02 | 0.4 |  |
| tokio | 1 | 1 | 40000 | 5 | yes | 1.565 | 8.782 | -0.2 | 0.02 | 0.4 |  |
| tokio | 1 | 1 | 60000 | 1 | yes | 2.478 | 15.761 | -1.3 | 0.01 | 0.1 |  |
| tokio | 1 | 1 | 60000 | 2 | yes | 3.363 | 19.415 | -1.3 | 0.01 | 0.1 |  |
| tokio | 1 | 1 | 60000 | 3 | yes | 3.670 | 22.446 | -2.2 | 0.01 | 0.1 |  |
| tokio | 1 | 1 | 60000 | 4 | yes | 3.404 | 17.154 | -2.1 | 0.01 | 0.1 |  |
| tokio | 1 | 1 | 60000 | 5 | yes | 3.199 | 18.547 | -6.5 | 0.01 | 0.1 |  |
| tokio | 1 | 1 | 80000 | 1 | yes | 4.702 | 29.622 | -5.2 | 0.00 | 0.2 |  |
| tokio | 1 | 1 | 80000 | 2 | yes | 4.162 | 21.430 | -2.4 | 0.01 | 0.2 |  |
| tokio | 1 | 1 | 80000 | 3 | yes | 4.588 | 23.888 | 7.6 | 0.00 | 0.2 |  |
| tokio | 1 | 1 | 80000 | 4 | yes | 5.214 | 24.625 | -9.9 | 0.00 | 0.1 |  |
| tokio | 1 | 1 | 80000 | 5 | yes | 5.194 | 32.358 | -2.3 | 0.00 | 0.2 |  |
| uring | 1 | 1 | 1000 | 1 | no | 0.154 | 0.369 | 0.0 | 0.24 | 0.5 | lateness ratio |
| uring | 1 | 1 | 1000 | 2 | no | 0.138 | 0.275 | 0.0 | 0.27 | 0.4 | lateness ratio |
| uring | 1 | 1 | 1000 | 3 | no | 0.139 | 0.287 | 0.0 | 0.26 | 0.5 | lateness ratio |
| uring | 1 | 1 | 1000 | 4 | no | 0.137 | 0.260 | 0.0 | 0.27 | 0.5 | lateness ratio |
| uring | 1 | 1 | 1000 | 5 | no | 0.133 | 0.290 | 0.0 | 0.25 | 0.5 | lateness ratio |
| uring | 1 | 1 | 2000 | 1 | no | 0.140 | 0.289 | 0.0 | 0.24 | 0.4 | lateness ratio |
| uring | 1 | 1 | 2000 | 2 | no | 0.137 | 0.275 | 0.0 | 0.26 | 0.4 | lateness ratio |
| uring | 1 | 1 | 2000 | 3 | no | 0.132 | 0.267 | 0.0 | 0.24 | 0.4 | lateness ratio |
| uring | 1 | 1 | 2000 | 4 | no | 0.125 | 0.244 | 0.0 | 0.25 | 0.4 | lateness ratio |
| uring | 1 | 1 | 2000 | 5 | no | 0.125 | 0.245 | 0.0 | 0.25 | 0.4 | lateness ratio |
| uring | 1 | 1 | 5000 | 1 | no | 0.149 | 0.321 | -0.2 | 0.17 | 0.4 | lateness ratio |
| uring | 1 | 1 | 5000 | 2 | no | 0.171 | 0.369 | -0.2 | 0.16 | 0.4 | lateness ratio |
| uring | 1 | 1 | 5000 | 3 | no | 0.130 | 0.301 | 0.0 | 0.16 | 0.3 | lateness ratio |
| uring | 1 | 1 | 5000 | 4 | no | 0.135 | 0.309 | 0.0 | 0.15 | 0.3 | lateness ratio |
| uring | 1 | 1 | 5000 | 5 | no | 0.130 | 0.329 | 0.0 | 0.14 | 0.4 | lateness ratio |
| uring | 1 | 1 | 10000 | 1 | no | 0.172 | 0.388 | -0.2 | 0.15 | 0.5 | lateness ratio |
| uring | 1 | 1 | 10000 | 2 | no | 0.197 | 0.455 | -0.2 | 0.14 | 0.6 | lateness ratio |
| uring | 1 | 1 | 10000 | 3 | no | 0.182 | 0.414 | -0.1 | 0.14 | 0.4 | lateness ratio |
| uring | 1 | 1 | 10000 | 4 | no | 0.176 | 0.403 | -0.1 | 0.13 | 0.5 | lateness ratio |
| uring | 1 | 1 | 10000 | 5 | no | 0.183 | 0.422 | -0.1 | 0.13 | 0.5 | lateness ratio |
| uring | 1 | 1 | 20000 | 1 | yes | 0.341 | 1.137 | -0.4 | 0.06 | 0.4 |  |
| uring | 1 | 1 | 20000 | 1 | yes | 0.188 | 0.757 | -0.2 | 0.07 | 0.3 |  |
| uring | 1 | 1 | 20000 | 2 | yes | 0.274 | 0.907 | -0.5 | 0.07 | 0.3 |  |
| uring | 1 | 1 | 20000 | 2 | yes | 0.338 | 1.071 | -0.2 | 0.07 | 0.3 |  |
| uring | 1 | 1 | 20000 | 3 | yes | 0.272 | 2.494 | -0.2 | 0.03 | 0.4 |  |
| uring | 1 | 1 | 20000 | 3 | yes | 0.309 | 0.899 | -0.2 | 0.08 | 0.4 |  |
| uring | 1 | 1 | 20000 | 4 | yes | 0.252 | 0.639 | -0.2 | 0.09 | 0.3 |  |
| uring | 1 | 1 | 20000 | 4 | yes | 0.270 | 0.695 | -0.2 | 0.10 | 0.4 |  |
| uring | 1 | 1 | 20000 | 5 | yes | 0.275 | 0.699 | -0.2 | 0.09 | 0.3 |  |
| uring | 1 | 1 | 20000 | 5 | yes | 0.295 | 1.579 | -0.5 | 0.05 | 0.4 |  |
| uring | 1 | 1 | 30000 | 1 | yes | 0.379 | 1.251 | -0.6 | 0.05 | 0.2 |  |
| uring | 1 | 1 | 30000 | 2 | yes | 0.664 | 2.937 | -1.6 | 0.03 | 0.3 |  |
| uring | 1 | 1 | 30000 | 3 | yes | 1.059 | 7.025 | -0.4 | 0.02 | 0.3 |  |
| uring | 1 | 1 | 30000 | 4 | yes | 0.918 | 4.338 | -1.5 | 0.03 | 0.3 |  |
| uring | 1 | 1 | 30000 | 5 | yes | 0.758 | 3.865 | -0.3 | 0.03 | 0.4 |  |
| uring | 1 | 1 | 40000 | 1 | yes | 0.948 | 4.280 | -0.2 | 0.02 | 0.2 |  |
| uring | 1 | 1 | 40000 | 2 | yes | 0.997 | 5.448 | -1.5 | 0.03 | 0.4 |  |
| uring | 1 | 1 | 40000 | 3 | yes | 0.904 | 5.927 | -0.6 | 0.03 | 0.4 |  |
| uring | 1 | 1 | 40000 | 4 | yes | 1.007 | 5.825 | -2.4 | 0.03 | 0.4 |  |
| uring | 1 | 1 | 40000 | 5 | yes | 1.137 | 6.115 | -0.7 | 0.03 | 0.6 |  |
| uring | 1 | 1 | 60000 | 1 | yes | 1.687 | 10.674 | -0.7 | 0.01 | 0.1 |  |
| uring | 1 | 1 | 60000 | 2 | yes | 2.128 | 12.354 | -2.6 | 0.01 | 0.1 |  |
| uring | 1 | 1 | 60000 | 3 | yes | 2.333 | 13.976 | -5.9 | 0.01 | 0.1 |  |
| uring | 1 | 1 | 60000 | 4 | yes | 2.200 | 13.484 | -2.7 | 0.01 | 0.1 |  |
| uring | 1 | 1 | 60000 | 5 | yes | 2.345 | 12.435 | -0.2 | 0.01 | 0.1 |  |
| uring | 1 | 1 | 80000 | 1 | yes | 2.193 | 15.204 | -7.2 | 0.01 | 0.1 |  |
| uring | 1 | 1 | 80000 | 2 | yes | 3.604 | 23.282 | -1.8 | 0.01 | 0.1 |  |
| uring | 1 | 1 | 80000 | 3 | yes | 4.317 | 28.705 | -1.9 | 0.00 | 0.2 |  |
| uring | 1 | 1 | 80000 | 4 | yes | 3.590 | 28.361 | -7.0 | 0.01 | 0.1 |  |
| uring | 1 | 1 | 80000 | 5 | yes | 3.664 | 23.003 | -6.1 | 0.01 | 0.2 |  |
| tokio | 4 | 1 | 1000 | 1 | no | 0.153 | 0.293 | 0.0 | 0.26 | 0.6 | lateness ratio |
| tokio | 4 | 1 | 1000 | 2 | no | 0.154 | 0.314 | 0.0 | 0.25 | 0.7 | lateness ratio |
| tokio | 4 | 1 | 1000 | 3 | no | 0.157 | 0.331 | 0.0 | 0.22 | 0.7 | lateness ratio |
| tokio | 4 | 1 | 1000 | 4 | no | 0.157 | 0.311 | 0.0 | 0.24 | 0.7 | lateness ratio |
| tokio | 4 | 1 | 1000 | 5 | no | 0.147 | 0.301 | 0.0 | 0.22 | 0.7 | lateness ratio |
| tokio | 4 | 1 | 2000 | 1 | no | 0.143 | 0.259 | 0.0 | 0.23 | 0.5 | lateness ratio |
| tokio | 4 | 1 | 2000 | 2 | no | 0.141 | 0.279 | 0.0 | 0.22 | 0.4 | lateness ratio |
| tokio | 4 | 1 | 2000 | 3 | no | 0.142 | 0.259 | 0.0 | 0.24 | 0.5 | lateness ratio |
| tokio | 4 | 1 | 2000 | 4 | no | 0.141 | 0.238 | 0.0 | 0.26 | 0.4 | lateness ratio |
| tokio | 4 | 1 | 2000 | 5 | no | 0.142 | 0.269 | 0.0 | 0.22 | 0.4 | lateness ratio |
| tokio | 4 | 1 | 5000 | 1 | no | 0.154 | 0.316 | 0.0 | 0.17 | 0.3 | lateness ratio |
| tokio | 4 | 1 | 5000 | 2 | no | 0.146 | 0.315 | 0.0 | 0.16 | 0.3 | lateness ratio |
| tokio | 4 | 1 | 5000 | 3 | no | 0.145 | 0.319 | 0.0 | 0.16 | 0.3 | lateness ratio |
| tokio | 4 | 1 | 5000 | 4 | no | 0.144 | 0.309 | 0.0 | 0.15 | 0.3 | lateness ratio |
| tokio | 4 | 1 | 5000 | 5 | no | 0.153 | 0.324 | 0.0 | 0.16 | 0.3 | lateness ratio |
| tokio | 4 | 1 | 10000 | 1 | no | 0.210 | 0.405 | -0.1 | 0.15 | 0.5 | lateness ratio |
| tokio | 4 | 1 | 10000 | 2 | no | 0.206 | 0.407 | -0.1 | 0.14 | 0.5 | lateness ratio |
| tokio | 4 | 1 | 10000 | 3 | no | 0.199 | 0.383 | -0.1 | 0.15 | 0.5 | lateness ratio |
| tokio | 4 | 1 | 10000 | 4 | no | 0.208 | 0.469 | -0.1 | 0.14 | 0.6 | lateness ratio |
| tokio | 4 | 1 | 10000 | 5 | no | 0.205 | 0.388 | -0.2 | 0.15 | 0.5 | lateness ratio |
| tokio | 4 | 1 | 20000 | 1 | no | 0.223 | 0.483 | -0.1 | 0.14 | 0.7 | lateness ratio |
| tokio | 4 | 1 | 20000 | 2 | yes | 0.236 | 1.513 | -0.2 | 0.06 | 0.9 |  |
| tokio | 4 | 1 | 20000 | 3 | no | 0.222 | 0.475 | -0.1 | 0.14 | 0.6 | lateness ratio |
| tokio | 4 | 1 | 20000 | 4 | no | 0.233 | 0.572 | -0.2 | 0.14 | 0.8 | lateness ratio |
| tokio | 4 | 1 | 20000 | 5 | no | 0.218 | 0.491 | -0.2 | 0.14 | 0.6 | lateness ratio |
| uring | 4 | 1 | 1000 | 1 | no | 0.140 | 0.287 | 0.0 | 0.27 | 0.4 | lateness ratio |
| uring | 4 | 1 | 1000 | 2 | no | 0.133 | 0.264 | 0.0 | 0.26 | 0.3 | lateness ratio |
| uring | 4 | 1 | 1000 | 3 | no | 0.136 | 0.279 | 0.0 | 0.29 | 0.3 | lateness ratio |
| uring | 4 | 1 | 1000 | 4 | no | 0.138 | 0.278 | 0.0 | 0.26 | 0.3 | lateness ratio |
| uring | 4 | 1 | 1000 | 5 | no | 0.133 | 0.263 | 0.0 | 0.26 | 0.3 | lateness ratio |
| uring | 4 | 1 | 2000 | 1 | no | 0.130 | 0.277 | 0.0 | 0.24 | 0.3 | lateness ratio |
| uring | 4 | 1 | 2000 | 2 | no | 0.124 | 0.223 | 0.0 | 0.27 | 0.3 | lateness ratio |
| uring | 4 | 1 | 2000 | 3 | no | 0.126 | 0.225 | 0.0 | 0.28 | 0.4 | lateness ratio |
| uring | 4 | 1 | 2000 | 4 | no | 0.132 | 0.287 | 0.0 | 0.21 | 0.3 | lateness ratio |
| uring | 4 | 1 | 2000 | 5 | no | 0.126 | 0.266 | 0.0 | 0.22 | 0.3 | lateness ratio |
| uring | 4 | 1 | 5000 | 1 | no | 0.137 | 0.297 | 0.0 | 0.18 | 0.3 | lateness ratio |
| uring | 4 | 1 | 5000 | 2 | no | 0.134 | 0.305 | 0.0 | 0.18 | 0.3 | lateness ratio |
| uring | 4 | 1 | 5000 | 3 | no | 0.130 | 0.299 | 0.0 | 0.17 | 0.3 | lateness ratio |
| uring | 4 | 1 | 5000 | 4 | no | 0.129 | 0.296 | 0.0 | 0.18 | 0.4 | lateness ratio |
| uring | 4 | 1 | 5000 | 5 | no | 0.134 | 0.279 | 0.0 | 0.20 | 0.3 | lateness ratio |
| uring | 4 | 1 | 10000 | 1 | no | 0.175 | 0.359 | -0.1 | 0.16 | 0.7 | lateness ratio |
| uring | 4 | 1 | 10000 | 2 | no | 0.183 | 0.384 | -0.1 | 0.15 | 0.6 | lateness ratio |
| uring | 4 | 1 | 10000 | 3 | no | 0.178 | 0.364 | -0.1 | 0.17 | 0.7 | lateness ratio |
| uring | 4 | 1 | 10000 | 4 | no | 0.181 | 0.369 | -0.1 | 0.18 | 0.7 | lateness ratio |
| uring | 4 | 1 | 10000 | 5 | no | 0.183 | 0.387 | -0.1 | 0.16 | 0.7 | lateness ratio |
| uring | 4 | 1 | 20000 | 1 | no | 0.190 | 0.420 | -0.1 | 0.18 | 1.0 | lateness ratio |
| uring | 4 | 1 | 20000 | 2 | no | 0.201 | 0.507 | -0.1 | 0.15 | 1.1 | lateness ratio |
| uring | 4 | 1 | 20000 | 3 | no | 0.195 | 0.430 | -0.1 | 0.17 | 0.9 | lateness ratio |
| uring | 4 | 1 | 20000 | 4 | no | 0.186 | 0.402 | -0.1 | 0.17 | 0.8 | lateness ratio |
| uring | 4 | 1 | 20000 | 5 | no | 0.200 | 0.446 | -0.1 | 0.16 | 0.9 | lateness ratio |

<!-- 387 runs -->
