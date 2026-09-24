//! Client identity.

use bytes::Bytes;

/// The identity a client presents when it connects. Protocols carry it as
/// text; length limits are protocol policy and are checked before this type
/// is constructed. May be empty: the broker then assigns an identity.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ClientId(Bytes);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("client id is not valid UTF-8")]
pub struct InvalidClientId;

impl ClientId {
    pub fn new(bytes: Bytes) -> Result<Self, InvalidClientId> {
        std::str::from_utf8(&bytes).map_err(|_| InvalidClientId)?;
        Ok(Self(bytes))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[expect(clippy::expect_used, reason = "UTF-8 checked in ClientId::new")]
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).expect("validated on construction")
    }
}

impl std::fmt::Debug for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClientId({:?})", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_utf8_keeps_empty() {
        assert!(ClientId::new(Bytes::from_static(b"\xff")).is_err());
        assert!(ClientId::new(Bytes::new()).unwrap().is_empty());
        assert_eq!(
            ClientId::new(Bytes::from_static(b"a b")).unwrap().as_str(),
            "a b"
        );
    }
}

/// One connection's session, allocated by the runtime when it accepts the
/// connection. Unrelated to [`ClientId`]: the broker maps between them, and a
/// reconnecting client gets a new `SessionId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionId(pub u64);
