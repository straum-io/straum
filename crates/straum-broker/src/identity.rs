//! The node's identity in its data directory (`identity`): the cluster id
//! and node id from the configuration of the first start. A later start
//! with another id is refused. Node-local, so not in the WAL: the WAL holds
//! what replicas share, and two replicas have different node ids.
//!
//! Layout: `magic "STRMNODE" | version u32 = 1 | node_id u32 | cluster_id
//! (u16 length, bytes) | crc32c u32` of everything before it.

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

const FILE: &str = "identity";
const TMP: &str = "identity.tmp";
const MAGIC: &[u8; 8] = b"STRMNODE";
const VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    pub cluster_id: String,
    pub node_id: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    #[error(
        "{dir}: the data directory belongs to cluster {stored:?}, node {stored_node}; the configuration says cluster {configured:?}, node {configured_node}"
    )]
    Mismatch {
        dir: String,
        stored: String,
        stored_node: u32,
        configured: String,
        configured_node: u32,
    },
    #[error("{0}: the identity file is damaged or of an unknown version")]
    Corrupt(String),
    #[error(transparent)]
    Io(#[from] io::Error),
}

fn encode(id: &Identity) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(MAGIC);
    b.extend_from_slice(&VERSION.to_le_bytes());
    b.extend_from_slice(&id.node_id.to_le_bytes());
    let name = id.cluster_id.as_bytes();
    b.extend_from_slice(&(name.len().min(u16::MAX as usize) as u16).to_le_bytes());
    b.extend_from_slice(&name[..name.len().min(u16::MAX as usize)]);
    let crc = crc32c::crc32c(&b);
    b.extend_from_slice(&crc.to_le_bytes());
    b
}

fn decode(b: &[u8]) -> Option<Identity> {
    let (body, crc) = b.split_at_checked(b.len().checked_sub(4)?)?;
    if crc32c::crc32c(body).to_le_bytes() != crc
        || body.get(..8)? != MAGIC
        || body.get(8..12)? != VERSION.to_le_bytes()
    {
        return None;
    }
    let node_id = u32::from_le_bytes(body.get(12..16)?.try_into().ok()?);
    let len = u16::from_le_bytes(body.get(16..18)?.try_into().ok()?) as usize;
    let name = body.get(18..)?;
    if name.len() != len {
        return None;
    }
    Some(Identity {
        cluster_id: String::from_utf8(name.to_vec()).ok()?,
        node_id,
    })
}

/// Checks `dir`'s identity against `configured`, or records it on the first
/// start (written to a temporary file, synced, renamed, directory synced).
/// Returns whether it was created.
pub fn check_or_create(dir: &Path, configured: &Identity) -> Result<bool, IdentityError> {
    let path = dir.join(FILE);
    match std::fs::read(&path) {
        Ok(bytes) => {
            let stored =
                decode(&bytes).ok_or_else(|| IdentityError::Corrupt(path.display().to_string()))?;
            if &stored != configured {
                return Err(IdentityError::Mismatch {
                    dir: dir.display().to_string(),
                    stored: stored.cluster_id,
                    stored_node: stored.node_id,
                    configured: configured.cluster_id.clone(),
                    configured_node: configured.node_id,
                });
            }
            Ok(false)
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let tmp = dir.join(TMP);
            let mut f = File::create(&tmp)?;
            f.write_all(&encode(configured))?;
            f.sync_all()?;
            std::fs::rename(&tmp, &path)?;
            File::open(dir)?.sync_all()?;
            Ok(true)
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_is_kept_and_checked() {
        let dir =
            std::env::temp_dir().join(format!("straum-broker-identity-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let id = Identity {
            cluster_id: "c1".into(),
            node_id: 7,
        };
        assert!(check_or_create(&dir, &id).unwrap(), "created");
        assert!(!check_or_create(&dir, &id).unwrap(), "same identity");
        let other = Identity {
            node_id: 8,
            ..id.clone()
        };
        assert!(matches!(
            check_or_create(&dir, &other),
            Err(IdentityError::Mismatch { .. })
        ));
        let mut bytes = std::fs::read(dir.join(FILE)).unwrap();
        bytes[12] ^= 1;
        std::fs::write(dir.join(FILE), &bytes).unwrap();
        assert!(matches!(
            check_or_create(&dir, &id),
            Err(IdentityError::Corrupt(_))
        ));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
