//! `bench server` subcommand (task 4 of the benchmark-harness feature):
//! runs the uring server or the tokio twin with a [`crate::record::Recorder`]
//! callback, completes the ARM handshake, and on SIGTERM writes a
//! [`crate::record::DeliveryReport`] with the drop total (AC-15, AC-16,
//! AC-51). Signatures copied from `features/benchmark-harness/PLAN.md`
//! `## Interface Contracts`.

use std::io::{self, BufRead, Read, Write};
use std::path::PathBuf;
use std::sync::mpsc;

use crate::record::{parse_arm_line, DeliveryReport, Recorder, CONNECTIONS_MAX};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerImpl {
    Uring,
    Tokio,
}

#[derive(Debug, Clone)]
pub struct ServerArgs {
    pub imp: ServerImpl,
    pub workers: usize,
    pub addr: String,
    pub connections: u32,
    pub report: PathBuf,
}

/// Longest stdin line the reader accepts. An `ARM` line is the command plus
/// three decimal `u64` values, so 128 bytes leaves generous slack. The limit
/// keeps a line without a newline from growing the reader's buffer without
/// bound before [`parse_arm_line`] can reject it.
const STDIN_LINE_MAX_BYTES: u64 = 128;

/// Control messages the start-handshake loop receives on its single channel:
/// `Arm(line)` from the stdin reader, `StdinClosed` when EOF arrives,
/// `ReadFailed(reason)` when the line cannot be read, and `Shutdown` from the
/// ctrlc handler.
enum Control {
    Arm(String),
    StdinClosed,
    ReadFailed(String),
    Shutdown,
}

pub fn run_server(args: &ServerArgs) -> Result<(), String> {
    // The connection count sizes one recorder shard per connection, and each
    // shard that records a latency allocates a histogram, so the count bounds
    // the memory the run can hold. A mistyped value must be refused here: past
    // this point an out-of-memory kill leaves the runner with a timeout
    // instead of a diagnostic. `CONNECTIONS_MAX` is that memory budget, and
    // the histograms are allocated per delivering connection rather than up
    // front, so no value this check accepts can exhaust memory before `READY`.
    if args.connections == 0 || args.connections > CONNECTIONS_MAX {
        return Err(format!(
            "connections must be between 1 and {CONNECTIONS_MAX}, got {}",
            args.connections
        ));
    }
    let recorder = Recorder::new(args.connections);
    let callback = recorder.callback();

    let config = uring_mqtt::BrokerConfig::new(args.addr.clone()).num_workers(args.workers);

    let (tx, rx) = mpsc::channel::<Control>();

    match args.imp {
        ServerImpl::Uring => {
            let handle = uring_mqtt::MqttBroker::start_with_callback(config, Some(callback))
                .map_err(|e| format!("start broker: {e}"))?;
            let trigger = handle.shutdown_handle();
            let dropped = handle.dropped_publishes_handle();
            let handler_tx = tx.clone();
            ctrlc::set_handler(move || {
                trigger.shutdown();
                let _ = handler_tx.send(Control::Shutdown);
            })
            .map_err(|e| format!("install ctrl-c handler: {e}"))?;
            spawn_stdin_reader(tx);
            println!("READY");
            io::stdout()
                .flush()
                .map_err(|e| format!("flush READY: {e}"))?;
            run_handshake(&recorder, &rx)?;
            // A worker error must not cost the run its delivered counts: write
            // the report on both outcomes, then report the join failure.
            let joined = handle.join().map_err(|e| format!("join broker: {e}"));
            let total = dropped.total();
            let mut report = recorder.report();
            report.dropped = Some(total);
            write_report(&args.report, &report)?;
            joined?;
        }
        ServerImpl::Tokio => {
            let handle = crate::twin::start(config, callback)
                .map_err(|e| format!("start twin: {e}"))?;
            let trigger = handle.shutdown_handle();
            let dropped = handle.dropped_publishes_handle();
            let handler_tx = tx.clone();
            ctrlc::set_handler(move || {
                trigger.shutdown();
                let _ = handler_tx.send(Control::Shutdown);
            })
            .map_err(|e| format!("install ctrl-c handler: {e}"))?;
            spawn_stdin_reader(tx);
            println!("READY");
            io::stdout()
                .flush()
                .map_err(|e| format!("flush READY: {e}"))?;
            run_handshake(&recorder, &rx)?;
            // Same as the uring branch, down to the drop total: the counters
            // behind `dropped` outlive the join, so a session error costs the
            // run its validity but not its accounting. `dropped: null` stays
            // reserved for the mosquitto subscriber (decision Q7).
            let joined = handle.join().map_err(|e| format!("join twin: {e}"));
            let total = dropped.total();
            let mut report = recorder.report();
            report.dropped = Some(total);
            write_report(&args.report, &report)?;
            joined?;
        }
    }
    Ok(())
}

/// Drain the control channel until a `Shutdown` arrives. `StdinClosed` keeps
/// the server unarmed and waiting; `Arm(line)` parses the line and arms
/// the recorder. Any read, parse or arm error short-circuits the handshake
/// with `Err`, and the caller returns it; dropping the server handle then
/// shuts the endpoint down and joins it, so the run ends without a report.
fn run_handshake(recorder: &Recorder, rx: &mpsc::Receiver<Control>) -> Result<(), String> {
    loop {
        match rx.recv() {
            Ok(Control::Arm(line)) => {
                let (origin_ns, window) = parse_arm_line(&line)?;
                recorder.arm(origin_ns, window)?;
                println!("ARMED");
                io::stdout()
                    .flush()
                    .map_err(|e| format!("flush ARMED: {e}"))?;
            }
            Ok(Control::StdinClosed) => {
                // Stay unarmed, keep waiting for a signal.
            }
            Ok(Control::ReadFailed(reason)) => return Err(reason),
            Ok(Control::Shutdown) => break,
            Err(_) => return Ok(()),
        }
    }
    Ok(())
}

/// Read one line at a time from stdin and forward it as a control message.
/// Uses blocking std I/O because the main thread cannot block in `read_line`
/// while ctrlc is installed with `SA_RESTART`: a signal there would not
/// advance the handshake loop. The thread is killed when the process exits.
/// Each read stops after [`STDIN_LINE_MAX_BYTES`], so an endless line is
/// reported instead of buffered.
fn spawn_stdin_reader(tx: mpsc::Sender<Control>) {
    std::thread::Builder::new()
        .name("bench-stdin".into())
        .spawn(move || {
            let stdin = io::stdin();
            let mut locked = stdin.lock();
            let mut line = String::new();
            loop {
                line.clear();
                match (&mut locked).take(STDIN_LINE_MAX_BYTES).read_line(&mut line) {
                    Ok(0) => {
                        let _ = tx.send(Control::StdinClosed);
                        break;
                    }
                    Err(e) => {
                        // A read error is not EOF: invalid UTF-8 on the ARM
                        // channel must end the run with its cause, not leave
                        // the handshake waiting for a signal.
                        let _ = tx.send(Control::ReadFailed(format!("read stdin line: {e}")));
                        break;
                    }
                    Ok(read) => {
                        // A full read without a newline means the limit cut the
                        // line short: the rest of it is still queued, so stop
                        // reading instead of parsing a tail as a new command.
                        let truncated = !line.ends_with('\n')
                            && u64::try_from(read).expect("read length fits in u64")
                                >= STDIN_LINE_MAX_BYTES;
                        let message = if truncated {
                            Control::ReadFailed(format!(
                                "stdin line is longer than {STDIN_LINE_MAX_BYTES} bytes"
                            ))
                        } else {
                            Control::Arm(line.clone())
                        };
                        let sent = tx.send(message).is_ok();
                        if truncated || !sent {
                            break;
                        }
                    }
                }
            }
        })
        .expect("spawn stdin reader");
}

fn write_report(path: &std::path::Path, report: &DeliveryReport) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| format!("create report dir: {e}"))?;
        }
    }
    let file = std::fs::File::create(path).map_err(|e| format!("create report: {e}"))?;
    let mut writer = io::BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, report).map_err(|e| format!("write report: {e}"))?;
    writer.flush().map_err(|e| format!("flush report: {e}"))?;
    Ok(())
}