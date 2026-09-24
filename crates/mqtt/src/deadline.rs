//! When a silent connection must be closed.
//!
//! Sans-IO like the rest of the crate: every `Instant` comes from the caller,
//! nothing here reads the clock or sleeps. The runtime arms a timer on
//! [`Deadline::at`] and closes the connection when it fires.

use std::time::{Duration, Instant};

use crate::MqttLimits;

/// Keep-alive tolerance: 1.5 × the interval the client asked for (§6).
const KEEP_ALIVE_NUMERATOR: u64 = 1500;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Waiting for CONNECT. The deadline runs from accept and is not
    /// extended by traffic: bytes that are not a CONNECT buy no time.
    AwaitingConnect,
    /// Connected. `None` means the client disabled keep-alive.
    Connected { tolerance: Option<Duration> },
}

/// The point in time at which one connection has been silent for too long.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deadline {
    phase: Phase,
    at: Option<Instant>,
}

impl Deadline {
    /// Starts the connect deadline for a freshly accepted connection.
    pub fn on_accept(now: Instant, limits: &MqttLimits) -> Self {
        Self {
            phase: Phase::AwaitingConnect,
            at: now.checked_add(limits.connect_timeout),
        }
    }

    /// Switches to keep-alive. `keep_alive_secs` is what CONNECT carried;
    /// 0 disables the deadline entirely.
    pub fn on_connect(&mut self, now: Instant, keep_alive_secs: u16) {
        let tolerance = tolerance(keep_alive_secs);
        self.phase = Phase::Connected { tolerance };
        self.arm(now);
    }

    /// Records that something arrived from the client.
    pub fn on_packet(&mut self, now: Instant) {
        if matches!(self.phase, Phase::Connected { .. }) {
            self.arm(now);
        }
    }

    /// When the connection must be closed if nothing arrives until then.
    /// `None` means it can stay silent forever.
    pub fn at(&self) -> Option<Instant> {
        self.at
    }

    pub fn is_expired(&self, now: Instant) -> bool {
        self.at.is_some_and(|at| now >= at)
    }

    fn arm(&mut self, now: Instant) {
        self.at = match self.phase {
            Phase::AwaitingConnect => self.at,
            Phase::Connected { tolerance } => {
                tolerance.and_then(|tolerance| now.checked_add(tolerance))
            }
        };
    }
}

fn tolerance(keep_alive_secs: u16) -> Option<Duration> {
    if keep_alive_secs == 0 {
        return None;
    }
    // 65535 × 1500 ms fits in u64 with room to spare.
    Some(Duration::from_millis(
        u64::from(keep_alive_secs).saturating_mul(KEEP_ALIVE_NUMERATOR),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> MqttLimits {
        MqttLimits::default()
    }

    fn base() -> Instant {
        Instant::now()
    }

    #[test]
    fn connect_deadline_is_absolute() {
        let start = base();
        let mut deadline = Deadline::on_accept(start, &limits());
        let at = deadline.at().unwrap();
        assert_eq!(at, start + Duration::from_millis(10_000));

        // Bytes that are not a CONNECT do not extend it.
        deadline.on_packet(start + Duration::from_secs(5));
        assert_eq!(deadline.at(), Some(at));

        assert!(!deadline.is_expired(at - Duration::from_millis(1)));
        assert!(deadline.is_expired(at));
    }

    #[test]
    fn keep_alive_tolerates_one_and_a_half_intervals() {
        let start = base();
        let mut deadline = Deadline::on_accept(start, &limits());
        deadline.on_connect(start, 10);

        let at = deadline.at().unwrap();
        assert_eq!(at, start + Duration::from_secs(15));
        assert!(!deadline.is_expired(start + Duration::from_millis(14_999)));
        assert!(deadline.is_expired(at));
    }

    #[test]
    fn every_packet_pushes_the_deadline_back() {
        let start = base();
        let mut deadline = Deadline::on_accept(start, &limits());
        deadline.on_connect(start, 10);

        for second in 1..100 {
            let now = start + Duration::from_secs(second);
            assert!(
                !deadline.is_expired(now),
                "closed at {second}s despite traffic"
            );
            deadline.on_packet(now);
        }

        let last = start + Duration::from_secs(99);
        assert_eq!(deadline.at(), Some(last + Duration::from_secs(15)));
        assert!(deadline.is_expired(last + Duration::from_secs(15)));
    }

    #[test]
    fn keep_alive_zero_disables_the_deadline() {
        let start = base();
        let mut deadline = Deadline::on_accept(start, &limits());
        deadline.on_connect(start, 0);

        assert_eq!(deadline.at(), None);
        assert!(!deadline.is_expired(start + Duration::from_secs(86_400)));

        deadline.on_packet(start + Duration::from_secs(1));
        assert_eq!(deadline.at(), None);
    }

    #[test]
    fn the_largest_keep_alive_stays_finite() {
        let start = base();
        let mut deadline = Deadline::on_accept(start, &limits());
        deadline.on_connect(start, u16::MAX);

        assert_eq!(
            deadline.at(),
            Some(start + Duration::from_millis(98_302_500))
        );
    }
}
