pub mod analysis;
pub mod doc;
pub mod gen;
pub mod matrix;
pub mod payload;
pub mod record;
pub mod run_record;
pub mod runner;
pub mod server;
pub mod sub;
pub mod twin;
pub mod wire;

use std::sync::{Mutex, PoisonError};

/// Ports already handed out in this process. The twin's workers bind with
/// `SO_REUSEPORT`, so two servers given the same port would both bind and
/// split each other's clients.
static ISSUED_PORTS: Mutex<Vec<u16>> = Mutex::new(Vec::new());

/// Hand out a TCP port that nothing else in this process has been given, or
/// the exhaustion cause. `runner::run_one` owes the matrix a record for
/// every spec, so the caller that cannot panic takes this path.
/// Mirrors `tests/common/mod.rs:28-44`.
pub fn try_free_port() -> Result<u16, String> {
    // Hand out a TCP port outside the ephemeral range (so per-test client
    // sockets in TIME_WAIT do not exhaust the kernel's `ip_local_port_range`)
    // and outside any port another broker in this process has been given.
    // Port 0 is rejected: the kernel can return an `EADDRINUSE` on bind
    // when the ephemeral range is saturated, even when nothing in our
    // process binds it. Searching a small private band keeps the test
    // independent of `/proc/sys/net/ipv4/ip_local_port_range`.
    const TRY_BASE: u16 = 20_000;
    const TRY_SPAN: u16 = 2_000;
    let mut guard = ISSUED_PORTS.lock().unwrap_or_else(PoisonError::into_inner);
    for offset in 0..TRY_SPAN {
        let port = TRY_BASE.wrapping_add(offset);
        let Ok(listener) = std::net::TcpListener::bind(std::net::SocketAddr::from((
            [127, 0, 0, 1],
            port,
        ))) else {
            continue;
        };
        let actual = listener.local_addr().expect("local_addr").port();
        drop(listener);
        if !guard.contains(&actual) {
            guard.push(actual);
            return Ok(actual);
        }
    }
    Err(format!(
        "free_port: exhausted {TRY_SPAN} ports in the {TRY_BASE}..={} band",
        TRY_BASE.wrapping_add(TRY_SPAN)
    ))
}

/// [`try_free_port`] for a test that has nothing to do with an exhausted
/// band: the panic names the band it searched.
pub fn free_port() -> u16 {
    match try_free_port() {
        Ok(port) => port,
        Err(e) => panic!("{e}"),
    }
}
