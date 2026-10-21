//! What one login costs.
//!
//! This number is the broker's connect capacity: a storm of N reconnecting
//! devices per second needs N times this much CPU, and nothing else the
//! broker does comes close. It is measured so an operator can size the
//! hashing parameters instead of guessing them.

use auth::{Authenticated, Authority};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

/// argon2id, the crate's defaults: m=19456 KiB, t=2, p=1.
const HASH: &str = "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHRzb21lc2FsdA$\
                    4xSKQ1Cz5rHEBZH1TkIhNRxrN2hMEa0Wo5Gy5tZ5rRE";

fn authority() -> Authority {
    match Authority::parse(&format!("device:{HASH}"), None, false) {
        Ok(authority) => authority,
        Err(error) => panic!("invalid benchmark credentials: {error}"),
    }
}

#[divan::bench]
fn verify_password(bencher: divan::Bencher) {
    let authority = authority();
    bencher.bench(|| {
        divan::black_box(authority.authenticate(
            Some(divan::black_box(b"device")),
            Some(divan::black_box(b"hunter2")),
        ))
    });
}

/// An unknown user must not be cheaper to reject than a known one is to
/// verify, or the difference times a few thousand attempts is a user list.
#[divan::bench]
fn reject_unknown_user(bencher: divan::Bencher) {
    let authority = authority();
    bencher.bench(|| divan::black_box(authority.authenticate(Some(b"nobody"), Some(b"hunter2"))));
}

#[divan::bench]
fn anonymous(bencher: divan::Bencher) {
    let authority = Authority::open();
    bencher.bench(|| {
        matches!(
            authority.authenticate(None, None),
            Authenticated::Allowed(_)
        )
    });
}
