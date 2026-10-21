//! Who may connect, and what they may do once connected.
//!
//! Pure policy: no I/O, no clock, no async. The files are read by `server`
//! and their contents parsed here, so the decisions are testable everywhere.
//!
//! Verifying a password is deliberately expensive — that is what a password
//! hash is for — so it never runs in the broker loop. The runtime does it on
//! a blocking thread, once per connection.

use std::collections::HashMap;

use argon2::password_hash::{PasswordHasher, SaltString, rand_core::OsRng};
use argon2::{Argon2, PasswordHash, PasswordVerifier};
use bytes::Bytes;
use domain::{Topic, TopicFilter};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("line {line}: expected `user:<password hash>`")]
    MalformedCredentials { line: usize },
    #[error("line {line}: {reason}")]
    MalformedRule { line: usize, reason: &'static str },
    #[error("line {line}: no user has been named yet")]
    RuleWithoutUser { line: usize },
    #[error("line {line}: {filter} is not a topic filter")]
    MalformedFilter { line: usize, filter: String },
    #[error("the password could not be hashed")]
    Unhashable,
    #[error("user {0} has rules but no password")]
    UnknownUser(String),
}

/// What a session is allowed to do. Held by the connection that owns it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Permissions {
    /// `None` means no restriction: an open broker, or a user with no rules.
    rules: Option<Vec<Rule>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Rule {
    filter: TopicFilter,
    publish: bool,
    subscribe: bool,
}

/// What a session wants to do with a topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Publish,
    Subscribe,
}

impl Permissions {
    /// Everything is allowed. Used when no ACL is configured at all.
    pub fn unrestricted() -> Self {
        Self { rules: None }
    }

    /// Nothing is allowed unless a rule says so.
    pub fn from_rules(rules: Vec<(TopicFilter, bool, bool)>) -> Self {
        Self {
            rules: Some(
                rules
                    .into_iter()
                    .map(|(filter, publish, subscribe)| Rule {
                        filter,
                        publish,
                        subscribe,
                    })
                    .collect(),
            ),
        }
    }

    /// Whether this session may publish to `topic`.
    pub fn may_publish(&self, topic: &Topic) -> bool {
        self.allows(topic, Action::Publish)
    }

    /// Whether this session may subscribe to `filter`.
    ///
    /// A subscription is allowed when a rule covers everything the filter
    /// could match — `sensors/#` does not grant `#`, because the wider filter
    /// would deliver topics the rule never mentioned.
    pub fn may_subscribe(&self, filter: &TopicFilter) -> bool {
        let Some(rules) = &self.rules else {
            return true;
        };
        rules
            .iter()
            .any(|rule| rule.subscribe && covers(&rule.filter, filter))
    }

    fn allows(&self, topic: &Topic, action: Action) -> bool {
        let Some(rules) = &self.rules else {
            return true;
        };
        rules.iter().any(|rule| {
            let permitted = match action {
                Action::Publish => rule.publish,
                Action::Subscribe => rule.subscribe,
            };
            permitted && rule.filter.matches(topic)
        })
    }
}

/// Whether everything `narrow` matches is also matched by `wide`.
///
/// Exact for the common shapes and conservative otherwise: an unclear case is
/// refused rather than granted.
fn covers(wide: &TopicFilter, narrow: &TopicFilter) -> bool {
    if wide == narrow {
        return true;
    }
    // `a/#` covers anything under `a/`, including wildcards, because `#`
    // already matches every topic below that point.
    let wide = wide.as_bytes();
    let narrow = narrow.as_bytes();
    let Some(prefix) = wide.strip_suffix(b"#") else {
        return false;
    };
    if prefix.is_empty() {
        // `#` covers everything except what the `$` rule hides, and a
        // subscriber of `#` never sees those anyway.
        return narrow.first() != Some(&b'$');
    }
    narrow.starts_with(prefix)
}

/// The credential file and the rules that go with it.
#[derive(Debug, Default)]
pub struct Authority {
    users: HashMap<String, User>,
    /// Whether a connection without a user name is allowed in.
    anonymous: Option<Permissions>,
}

#[derive(Debug)]
struct User {
    hash: String,
    permissions: Permissions,
}

/// The outcome of checking one CONNECT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authenticated {
    Allowed(Permissions),
    /// The user name is unknown or the password does not match.
    BadCredentials,
    /// Anonymous connections are not allowed here.
    NotAuthorized,
}

impl Authority {
    /// An open broker: anybody may connect and do anything.
    pub fn open() -> Self {
        Self {
            users: HashMap::new(),
            anonymous: Some(Permissions::unrestricted()),
        }
    }

    /// Builds from the contents of the password file and the ACL file.
    ///
    /// `acl` empty means every authenticated user may do anything; a user
    /// that appears in the ACL is restricted to their rules.
    pub fn parse(passwords: &str, acl: &str, anonymous: bool) -> Result<Self, AuthError> {
        let mut users = HashMap::new();

        for (index, line) in passwords.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((name, hash)) = line.split_once(':') else {
                return Err(AuthError::MalformedCredentials { line: index + 1 });
            };
            // A hash that does not parse could never match: refused at
            // startup, not found out one failed login at a time.
            if name.is_empty() || PasswordHash::new(hash).is_err() {
                return Err(AuthError::MalformedCredentials { line: index + 1 });
            }
            users.insert(
                name.to_owned(),
                User {
                    hash: hash.to_owned(),
                    permissions: Permissions::unrestricted(),
                },
            );
        }

        let mut anonymous_rules: Option<Vec<(TopicFilter, bool, bool)>> = None;
        let mut current: Option<String> = None;
        let mut rules: HashMap<String, Vec<(TopicFilter, bool, bool)>> = HashMap::new();

        for (index, line) in acl.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut words = line.split_whitespace();
            let Some(keyword) = words.next() else {
                continue;
            };

            match keyword {
                "user" => {
                    let Some(name) = words.next() else {
                        return Err(AuthError::MalformedRule {
                            line: index + 1,
                            reason: "expected `user <name>`",
                        });
                    };
                    if !users.contains_key(name) {
                        return Err(AuthError::UnknownUser(name.to_owned()));
                    }
                    rules.entry(name.to_owned()).or_default();
                    current = Some(name.to_owned());
                }
                "anonymous" => {
                    anonymous_rules.get_or_insert_with(Vec::new);
                    current = None;
                }
                "publish" | "subscribe" | "readwrite" => {
                    let Some(pattern) = words.next() else {
                        return Err(AuthError::MalformedRule {
                            line: index + 1,
                            reason: "expected a topic filter after the permission",
                        });
                    };
                    let filter = TopicFilter::new(Bytes::copy_from_slice(pattern.as_bytes()))
                        .map_err(|_| AuthError::MalformedFilter {
                            line: index + 1,
                            filter: pattern.to_owned(),
                        })?;
                    let entry = (
                        filter,
                        matches!(keyword, "publish" | "readwrite"),
                        matches!(keyword, "subscribe" | "readwrite"),
                    );

                    match (&current, &mut anonymous_rules) {
                        (Some(name), _) => rules.entry(name.clone()).or_default().push(entry),
                        (None, Some(anonymous)) => anonymous.push(entry),
                        (None, None) => {
                            return Err(AuthError::RuleWithoutUser { line: index + 1 });
                        }
                    }
                }
                _ => {
                    return Err(AuthError::MalformedRule {
                        line: index + 1,
                        reason: "expected `user`, `anonymous`, `publish`, `subscribe` or `readwrite`",
                    });
                }
            }
        }

        for (name, entries) in rules {
            if let Some(user) = users.get_mut(&name) {
                user.permissions = Permissions::from_rules(entries);
            }
        }

        let anonymous = match (anonymous, anonymous_rules) {
            (false, _) => None,
            (true, Some(entries)) => Some(Permissions::from_rules(entries)),
            (true, None) => Some(Permissions::unrestricted()),
        };

        Ok(Self { users, anonymous })
    }

    /// Whether anybody may connect without a user name.
    pub fn allows_anonymous(&self) -> bool {
        self.anonymous.is_some()
    }

    pub fn users(&self) -> usize {
        self.users.len()
    }

    /// Checks one CONNECT. Expensive by design: this is a password hash.
    pub fn authenticate(&self, username: Option<&[u8]>, password: Option<&[u8]>) -> Authenticated {
        let Some(username) = username else {
            return match &self.anonymous {
                Some(permissions) => Authenticated::Allowed(permissions.clone()),
                None => Authenticated::NotAuthorized,
            };
        };

        // Every refusal costs one hash, like an accepted login: a refusal
        // in microseconds where another costs tens of milliseconds tells
        // which user names exist, one guess at a time. So an unknown user,
        // a name that is not UTF-8 and a missing password all verify
        // against a throwaway hash.
        let user = std::str::from_utf8(username)
            .ok()
            .and_then(|name| self.users.get(name));
        let hash = user.and_then(|u| PasswordHash::new(&u.hash).ok());
        match (user, hash, password) {
            (Some(user), Some(hash), Some(password)) => {
                worked();
                match Argon2::default().verify_password(password, &hash) {
                    Ok(()) => Authenticated::Allowed(user.permissions.clone()),
                    Err(_) => Authenticated::BadCredentials,
                }
            }
            _ => {
                let _ = self.burn(password);
                Authenticated::BadCredentials
            }
        }
    }
}

/// One password hash computed, for the tests that check every path pays it.
#[cfg(test)]
static WORKED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn worked() {
    #[cfg(test)]
    WORKED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

impl Authority {
    /// Hashes a password for the credential file.
    ///
    /// The broker needs this to be usable at all: without a way to produce a
    /// hash there is no way to write the file.
    pub fn hash_password(password: &[u8]) -> Result<String, AuthError> {
        let salt = SaltString::generate(&mut OsRng);
        Argon2::default()
            .hash_password(password, &salt)
            .map(|hash| hash.to_string())
            .map_err(|_| AuthError::Unhashable)
    }

    /// Spends the same time an unknown user would have cost, so the answer
    /// carries no timing.
    fn burn(&self, password: Option<&[u8]>) -> bool {
        worked();
        const DECOY: &str = "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHRzb21lc2FsdA$\
                             4xSKQ1Cz5rHEBZH1TkIhNRxrN2hMEa0Wo5Gy5tZ5rRE";
        let Ok(hash) = PasswordHash::new(DECOY) else {
            return false;
        };
        Argon2::default()
            .verify_password(password.unwrap_or(b""), &hash)
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::sync::atomic::Ordering;

    use super::*;

    /// Every refusal and every acceptance costs exactly one hash, so none is
    /// told apart by its time (D-158 A).
    #[test]
    fn every_answer_to_a_user_name_costs_one_hash() {
        let hash = Authority::hash_password(b"right").unwrap();
        let authority = Authority::parse(&format!("alice:{hash}\n"), "", false).unwrap();
        for (user, password) in [
            (Some(&b"alice"[..]), Some(&b"right"[..])),
            (Some(b"alice"), Some(b"wrong")),
            (Some(b"alice"), None),
            (Some(b"mallory"), Some(b"right")),
            (Some(b"mallory"), None),
            (Some(&[0xff, 0xfe][..]), Some(b"right")),
        ] {
            let before = WORKED.load(Ordering::Relaxed);
            let _ = authority.authenticate(user, password);
            assert_eq!(
                WORKED.load(Ordering::Relaxed) - before,
                1,
                "{user:?} / {password:?}"
            );
        }
    }

    #[test]
    fn a_stored_hash_that_does_not_parse_refuses_the_file() {
        assert_eq!(
            Authority::parse("alice:garbage\n", "", false).err(),
            Some(AuthError::MalformedCredentials { line: 1 })
        );
    }
}
