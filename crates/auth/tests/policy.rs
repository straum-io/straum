//! Who gets in, and what they may touch afterwards.
#![allow(clippy::unwrap_used)]

use std::time::Instant;

use auth::{Authenticated, Authority, Permissions};
use bytes::Bytes;
use domain::{Topic, TopicFilter};

fn topic(value: &str) -> Topic {
    Topic::new(Bytes::copy_from_slice(value.as_bytes())).unwrap()
}

fn filter(value: &str) -> TopicFilter {
    TopicFilter::new(Bytes::copy_from_slice(value.as_bytes())).unwrap()
}

fn passwords() -> String {
    let alice = Authority::hash_password(b"alice-secret").unwrap();
    let bob = Authority::hash_password(b"bob-secret").unwrap();
    format!("# devices\nalice:{alice}\n\nbob:{bob}\n")
}

#[test]
fn a_correct_password_gets_in_and_a_wrong_one_does_not() {
    let authority = Authority::parse(&passwords(), None, false).unwrap();
    assert_eq!(authority.users(), 2);

    assert!(matches!(
        authority.authenticate(Some(b"alice"), Some(b"alice-secret")),
        Authenticated::Allowed(_)
    ));
    assert_eq!(
        authority.authenticate(Some(b"alice"), Some(b"bob-secret")),
        Authenticated::BadCredentials
    );
    assert_eq!(
        authority.authenticate(Some(b"alice"), None),
        Authenticated::BadCredentials
    );
    assert_eq!(
        authority.authenticate(Some(b"mallory"), Some(b"anything")),
        Authenticated::BadCredentials
    );
}

#[test]
fn anonymous_is_refused_unless_it_is_allowed() {
    let closed = Authority::parse(&passwords(), None, false).unwrap();
    assert_eq!(
        closed.authenticate(None, None),
        Authenticated::NotAuthorized
    );
    assert!(!closed.allows_anonymous());

    let open = Authority::parse(&passwords(), None, true).unwrap();
    assert!(matches!(
        open.authenticate(None, None),
        Authenticated::Allowed(_)
    ));
    assert!(open.allows_anonymous());
}

/// A rejection that is far cheaper than a success tells an attacker which
/// user names exist.
#[test]
fn an_unknown_user_costs_what_a_known_one_costs() {
    let authority = Authority::parse(&passwords(), None, false).unwrap();

    let started = Instant::now();
    let _ = authority.authenticate(Some(b"alice"), Some(b"wrong-password"));
    let known = started.elapsed();

    let started = Instant::now();
    let _ = authority.authenticate(Some(b"nobody-at-all"), Some(b"wrong-password"));
    let unknown = started.elapsed();

    // Same order of magnitude; the point is that the unknown user is hashed
    // too, not that the two are identical.
    assert!(
        unknown.as_micros() * 4 > known.as_micros(),
        "unknown user took {unknown:?} against {known:?} for a known one"
    );
}

#[test]
fn without_rules_a_user_may_do_anything() {
    let authority = Authority::parse(&passwords(), None, false).unwrap();
    let Authenticated::Allowed(permissions) =
        authority.authenticate(Some(b"bob"), Some(b"bob-secret"))
    else {
        panic!("bob should be allowed in");
    };

    assert!(permissions.may_publish(&topic("anything/at/all")));
    assert!(permissions.may_subscribe(&filter("#")));
}

#[test]
fn rules_restrict_the_user_they_name() {
    let acl = "\
# alice runs the sensors
user alice
publish sensors/+/temp
subscribe sensors/#

user bob
readwrite shared/bus
";
    let authority = Authority::parse(&passwords(), Some(acl), false).unwrap();

    let Authenticated::Allowed(alice) =
        authority.authenticate(Some(b"alice"), Some(b"alice-secret"))
    else {
        panic!("alice should be allowed in");
    };
    assert!(alice.may_publish(&topic("sensors/eu/temp")));
    assert!(!alice.may_publish(&topic("sensors/eu/humidity")));
    assert!(!alice.may_publish(&topic("other/topic")));
    assert!(alice.may_subscribe(&filter("sensors/eu/temp")));
    assert!(alice.may_subscribe(&filter("sensors/#")));
    // A wider subscription would deliver topics no rule mentions.
    assert!(!alice.may_subscribe(&filter("#")));
    assert!(!alice.may_subscribe(&filter("other/#")));
    // subscribe-only means publish is refused even where reading is fine.
    assert!(
        !alice.may_publish(&topic("sensors/eu/state"))
            || alice.may_publish(&topic("sensors/eu/temp"))
    );

    let Authenticated::Allowed(bob) = authority.authenticate(Some(b"bob"), Some(b"bob-secret"))
    else {
        panic!("bob should be allowed in");
    };
    assert!(bob.may_publish(&topic("shared/bus")));
    assert!(bob.may_subscribe(&filter("shared/bus")));
    assert!(!bob.may_publish(&topic("sensors/eu/temp")));
}

#[test]
fn anonymous_can_be_restricted_too() {
    let acl = "anonymous\nsubscribe public/#\n";
    let authority = Authority::parse(&passwords(), Some(acl), true).unwrap();

    let Authenticated::Allowed(permissions) = authority.authenticate(None, None) else {
        panic!("anonymous should be allowed in");
    };
    assert!(permissions.may_subscribe(&filter("public/news")));
    assert!(!permissions.may_publish(&topic("public/news")));
    assert!(!permissions.may_subscribe(&filter("private/#")));
}

#[test]
fn a_broken_file_is_an_error_naming_the_line() {
    assert!(Authority::parse("alice", None, false).is_err());
    assert!(Authority::parse(":hash", None, false).is_err());
    assert!(Authority::parse("alice:", None, false).is_err());

    let good = passwords();
    // A rule for somebody who has no password is a typo, not a permission.
    assert!(Authority::parse(&good, Some("user carol\npublish a/b\n"), false).is_err());
    // A rule before any user has been named belongs to nobody.
    assert!(Authority::parse(&good, Some("publish a/b\n"), false).is_err());
    assert!(Authority::parse(&good, Some("user alice\npublish a/#/b\n"), false).is_err());
    assert!(Authority::parse(&good, Some("user alice\nforbid a/b\n"), false).is_err());
}

#[test]
fn an_open_broker_allows_everything() {
    let authority = Authority::open();
    let Authenticated::Allowed(permissions) = authority.authenticate(None, None) else {
        panic!("an open broker lets anybody in");
    };
    assert!(permissions.may_publish(&topic("$SYS/anything")));
    assert_eq!(permissions, Permissions::unrestricted());
}

/// D-159 A: an ACL file grants only what it lists. A user it does not name
/// may do nothing; without an `anonymous` section, neither may anonymous
/// clients; an empty file grants nothing to anyone.
#[test]
fn an_acl_file_grants_only_what_it_lists() {
    let authority =
        Authority::parse(&passwords(), Some("user alice\nreadwrite a/#\n"), true).unwrap();
    let Authenticated::Allowed(bob) = authority.authenticate(Some(b"bob"), Some(b"bob-secret"))
    else {
        panic!("bob still gets in");
    };
    assert!(bob.grants_nothing());
    assert!(!bob.may_publish(&topic("a/b")));
    assert!(!bob.may_subscribe(&filter("a/#")));
    let Authenticated::Allowed(anonymous) = authority.authenticate(None, None) else {
        panic!("anonymous clients are let in");
    };
    assert!(anonymous.grants_nothing());
    assert!(!anonymous.may_publish(&topic("a/b")));

    let empty = Authority::parse(&passwords(), Some(""), false).unwrap();
    let Authenticated::Allowed(alice) = empty.authenticate(Some(b"alice"), Some(b"alice-secret"))
    else {
        panic!("alice still gets in");
    };
    assert!(!alice.may_publish(&topic("anything")));
    assert!(!alice.may_subscribe(&filter("#")));

    // Without a file, everybody may do anything, as before.
    let open = Authority::parse(&passwords(), None, true).unwrap();
    let Authenticated::Allowed(bob) = open.authenticate(Some(b"bob"), Some(b"bob-secret")) else {
        panic!("bob gets in");
    };
    assert!(bob.may_publish(&topic("a/b")));
}

#[test]
fn a_configuration_that_grants_nobody_anything_says_so() {
    // Anonymous clients let in, granted nothing, and no user.
    let nobody = Authority::parse("", Some("# no anonymous section\n"), true).unwrap();
    assert!(nobody.grants_nothing());
    let someone = Authority::parse("", Some("anonymous\nsubscribe public/#\n"), true).unwrap();
    assert!(!someone.grants_nothing());
    assert!(
        !Authority::parse(&passwords(), Some(""), false)
            .unwrap()
            .grants_nothing(),
        "users with no rules are still users: they may connect"
    );
}
