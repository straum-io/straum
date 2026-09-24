//! Hierarchical topics and topic filters.
//!
//! The model is the broker's own: levels separated by `/`, `+` matching one
//! level, `#` matching the rest. A protocol implementation maps its own
//! syntax onto these types; MQTT happens to use the same characters, but
//! nothing here is MQTT.
//!
//! Both types are validated on construction and cheap to clone. Size limits
//! (bytes, levels) are protocol policy and are enforced by the protocol crate
//! before it calls into here.

use bytes::Bytes;

const SEPARATOR: u8 = b'/';
const SINGLE: &[u8] = b"+";
const MULTI: &[u8] = b"#";

/// Rejected topic or topic filter. The input is not retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TopicError {
    #[error("empty")]
    Empty,
    #[error("not valid UTF-8")]
    NotUtf8,
    #[error("a topic must not contain `+` or `#`")]
    WildcardInTopic,
    #[error("`+` and `#` must occupy a whole level")]
    WildcardWithinLevel,
    #[error("`#` must be the last level")]
    MultiLevelNotLast,
}

/// A topic a message is published to. Never contains a wildcard.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Topic(Bytes);

/// A subscription filter. May contain `+` and `#`.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TopicFilter(Bytes);

impl Topic {
    /// Fails on an empty topic, invalid UTF-8, or any wildcard character.
    pub fn new(bytes: Bytes) -> Result<Self, TopicError> {
        validate_utf8(&bytes)?;
        if bytes.contains(&SINGLE[0]) || bytes.contains(&MULTI[0]) {
            return Err(TopicError::WildcardInTopic);
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn as_str(&self) -> &str {
        as_str(&self.0)
    }
}

impl TopicFilter {
    /// Fails on an empty filter, invalid UTF-8, a wildcard sharing a level
    /// with other characters, or `#` anywhere but last.
    pub fn new(bytes: Bytes) -> Result<Self, TopicError> {
        validate_utf8(&bytes)?;

        let mut levels = bytes.split(|&b| b == SEPARATOR).peekable();
        while let Some(level) = levels.next() {
            if level.contains(&MULTI[0]) {
                if level != MULTI {
                    return Err(TopicError::WildcardWithinLevel);
                }
                if levels.peek().is_some() {
                    return Err(TopicError::MultiLevelNotLast);
                }
            } else if level.contains(&SINGLE[0]) && level != SINGLE {
                return Err(TopicError::WildcardWithinLevel);
            }
        }

        Ok(Self(bytes))
    }

    /// Whether `topic` is delivered to a subscriber of this filter.
    ///
    /// Allocation-free: both sides are walked level by level.
    pub fn matches(&self, topic: &Topic) -> bool {
        // Topics starting with `$` are reserved and stay invisible to
        // wildcards at the first level; an explicit `$...` filter reaches them.
        if topic.0.first() == Some(&b'$') {
            let first = self.0.split(|&b| b == SEPARATOR).next();
            if first == Some(MULTI) || first == Some(SINGLE) {
                return false;
            }
        }

        let mut topic_levels = topic.0.split(|&b| b == SEPARATOR);

        for filter_level in self.0.split(|&b| b == SEPARATOR) {
            // `#` also covers the level it sits on: `a/b/#` matches `a/b`.
            if filter_level == MULTI {
                return true;
            }
            match topic_levels.next() {
                Some(level) if filter_level == SINGLE || filter_level == level => continue,
                _ => return false,
            }
        }

        topic_levels.next().is_none()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn as_str(&self) -> &str {
        as_str(&self.0)
    }
}

fn validate_utf8(bytes: &Bytes) -> Result<(), TopicError> {
    if bytes.is_empty() {
        return Err(TopicError::Empty);
    }
    std::str::from_utf8(bytes).map_err(|_| TopicError::NotUtf8)?;
    Ok(())
}

#[expect(
    clippy::expect_used,
    reason = "UTF-8 checked in Topic::new / TopicFilter::new"
)]
fn as_str(bytes: &Bytes) -> &str {
    std::str::from_utf8(bytes).expect("validated on construction")
}

impl std::fmt::Debug for Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Topic({:?})", self.as_str())
    }
}

impl std::fmt::Debug for TopicFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TopicFilter({:?})", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn topic(s: &str) -> Topic {
        Topic::new(Bytes::copy_from_slice(s.as_bytes())).unwrap()
    }

    fn filter(s: &str) -> TopicFilter {
        TopicFilter::new(Bytes::copy_from_slice(s.as_bytes())).unwrap()
    }

    fn matches(t: &str, f: &str) -> bool {
        filter(f).matches(&topic(t))
    }

    #[test]
    fn topic_rejects_invalid() {
        let cases = [
            ("", TopicError::Empty),
            ("+wrong", TopicError::WildcardInTopic),
            ("wro#ng", TopicError::WildcardInTopic),
            ("w/r/o/n/g+", TopicError::WildcardInTopic),
            ("wrong/#/path", TopicError::WildcardInTopic),
        ];
        for (input, expected) in cases {
            let got = Topic::new(Bytes::copy_from_slice(input.as_bytes()));
            assert_eq!(got.unwrap_err(), expected, "{input:?}");
        }
        assert_eq!(
            Topic::new(Bytes::from_static(b"a/\xff")).unwrap_err(),
            TopicError::NotUtf8
        );
    }

    #[test]
    fn topic_accepts_empty_levels() {
        for input in ["a", "/a", "a/", "a//b", "/", "$SYS/broker/uptime"] {
            assert_eq!(topic(input).as_str(), input);
        }
    }

    #[test]
    fn filter_rejects_invalid() {
        let cases = [
            ("", TopicError::Empty),
            ("wrong/#/filter", TopicError::MultiLevelNotLast),
            ("#/wrong", TopicError::MultiLevelNotLast),
            ("wrong/wr#ng/filter", TopicError::WildcardWithinLevel),
            ("wrong/filter#", TopicError::WildcardWithinLevel),
            ("wr/o+/ng", TopicError::WildcardWithinLevel),
            ("wr/+o+/ng", TopicError::WildcardWithinLevel),
            ("wron/+g", TopicError::WildcardWithinLevel),
            ("##", TopicError::WildcardWithinLevel),
            ("++", TopicError::WildcardWithinLevel),
        ];
        for (input, expected) in cases {
            let got = TopicFilter::new(Bytes::copy_from_slice(input.as_bytes()));
            assert_eq!(got.unwrap_err(), expected, "{input:?}");
        }
    }

    #[test]
    fn filter_accepts_valid() {
        for input in [
            "#",
            "+",
            "a/b/c",
            "correct/filter/#",
            "cor/+/rect/+",
            "+/a/#",
            "/+",
        ] {
            assert_eq!(filter(input).as_str(), input);
        }
    }

    #[test]
    fn exact_and_wildcard_matching() {
        assert!(matches("a/b/c", "a/b/c"));
        assert!(!matches("a/b/c", "d/b/c"));
        assert!(!matches("a/b/c", "a/b/e"));
        assert!(!matches("a/b/c", "a/b/c/d"));
        assert!(!matches("a/b/c/d", "a/b/c"));

        assert!(matches("a/b/c", "a/+/c"));
        assert!(matches("a/b/c/d/e", "a/+/c/+/e"));
        assert!(!matches("a/b", "a/b/+"));
        assert!(matches("a/", "a/+"));

        assert!(matches("a/b/c", "#"));
        assert!(matches("a/b/c/d", "a/b/c/#"));
        assert!(matches("a/b/c/d/e/f", "a/b/c/#"));
        // 3.1.1 §4.7.1.2: `#` also matches the parent level itself.
        assert!(matches("a/b/c", "a/b/c/#"));
        assert!(!matches("a", "a/b/#"));
    }

    #[test]
    fn dollar_topics_are_hidden_from_leading_wildcards() {
        assert!(!matches("$SYS/uptime", "#"));
        assert!(!matches("$SYS/uptime", "+/uptime"));
        // An explicit filter reaches them; only the first level is reserved.
        assert!(matches("$SYS/uptime", "$SYS/uptime"));
        assert!(matches("$SYS/uptime", "$SYS/#"));
        assert!(matches("$SYS/uptime", "$SYS/+"));
        // The rule is about the first level only.
        assert!(matches("a/$SYS", "+/+"));
        assert!(matches("sy$tem/metrics", "sy$tem/+"));
    }

    /// Naive, allocating implementation used only as a test oracle.
    fn reference(t: &str, f: &str) -> bool {
        let levels: Vec<&str> = t.split('/').collect();
        let pattern: Vec<&str> = f.split('/').collect();
        if levels.first().is_some_and(|l| l.starts_with('$'))
            && matches!(pattern.first(), Some(&"#") | Some(&"+"))
        {
            return false;
        }
        fn walk(levels: &[&str], pattern: &[&str]) -> bool {
            match (levels.first(), pattern.first()) {
                (_, Some(&"#")) => true,
                (Some(level), Some(p)) if *p == "+" || p == level => {
                    walk(&levels[1..], &pattern[1..])
                }
                (None, None) => true,
                _ => false,
            }
        }
        walk(&levels, &pattern)
    }

    fn level() -> impl Strategy<Value = String> {
        prop_oneof!["a", "b", "\\$SYS", "", "\\+x"].prop_map(String::from)
    }

    fn pattern_level() -> impl Strategy<Value = String> {
        prop_oneof!["a", "b", "\\$SYS", "", "\\+", "#"].prop_map(String::from)
    }

    proptest! {
        #[test]
        fn matches_agrees_with_reference(
            t in prop::collection::vec(level(), 1..6),
            f in prop::collection::vec(pattern_level(), 1..6),
        ) {
            let t = t.join("/");
            let f = f.join("/");
            let (Ok(topic), Ok(filter)) = (
                Topic::new(Bytes::copy_from_slice(t.as_bytes())),
                TopicFilter::new(Bytes::copy_from_slice(f.as_bytes())),
            ) else {
                return Ok(());
            };
            prop_assert_eq!(filter.matches(&topic), reference(&t, &f), "{} vs {}", t, f);
        }

        #[test]
        fn construction_never_panics(raw in prop::collection::vec(any::<u8>(), 0..32)) {
            let bytes = Bytes::from(raw);
            let _ = Topic::new(bytes.clone());
            let _ = TopicFilter::new(bytes);
        }
    }
}
