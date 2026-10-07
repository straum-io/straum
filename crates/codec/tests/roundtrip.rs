//! Any packet either side may send, in either version, survives encode then
//! decode unchanged; arbitrary bytes never panic the decoder.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::num::{NonZeroU16, NonZeroU32};

use bytes::{Bytes, BytesMut};
use codec::*;
use domain::{ClientId, Topic, TopicFilter};
use proptest::prelude::*;

fn text() -> impl Strategy<Value = Bytes> {
    "[a-z0-9 ]{0,8}".prop_map(Bytes::from)
}

fn binary() -> impl Strategy<Value = Bytes> {
    proptest::collection::vec(any::<u8>(), 0..12).prop_map(Bytes::from)
}

fn topic() -> impl Strategy<Value = Topic> {
    "[a-z]{1,5}(/[a-z]{1,5}){0,3}".prop_map(|s| Topic::new(Bytes::from(s)).unwrap())
}

fn filter() -> impl Strategy<Value = TopicFilter> {
    prop_oneof![
        "[a-z]{1,5}(/[a-z]{1,5}){0,3}",
        "[a-z]{1,5}/\\+/[a-z]{1,5}",
        "[a-z]{1,5}/#",
        Just("#".to_string()),
    ]
    .prop_map(|s| TopicFilter::new(Bytes::from(s)).unwrap())
}

fn user(v5: bool) -> impl Strategy<Value = Vec<UserProperty>> {
    let max = if v5 { 3 } else { 0 };
    proptest::collection::vec(
        (text(), text()).prop_map(|(name, value)| UserProperty { name, value }),
        0..=max,
    )
}

fn version() -> impl Strategy<Value = Version> {
    prop_oneof![Just(Version::V311), Just(Version::V5)]
}

fn qos() -> impl Strategy<Value = Qos> {
    prop_oneof![
        Just(Qos::AtMostOnce),
        Just(Qos::AtLeastOnce),
        Just(Qos::ExactlyOnce)
    ]
}

fn id() -> impl Strategy<Value = PacketId> {
    (1u16..).prop_map(|v| PacketId::new(v).unwrap())
}

fn opt<T: std::fmt::Debug + Clone>(
    v5: bool,
    s: impl Strategy<Value = T>,
) -> impl Strategy<Value = Option<T>> {
    proptest::option::of(s).prop_map(move |v| if v5 { v } else { None })
}

fn connect(v: Version) -> impl Strategy<Value = Packet> {
    let v5 = v == Version::V5;
    let properties = (
        any::<u32>(),
        any::<NonZeroU16>(),
        opt(v5, any::<NonZeroU32>()),
        any::<u16>(),
        any::<bool>(),
        any::<bool>(),
        opt(v5, text()),
        opt(v5, binary()),
        user(v5),
    )
        .prop_map(move |(se, rm, mps, tam, rri, rpi, am, ad, user)| {
            if !v5 {
                return ConnectProperties::default();
            }
            ConnectProperties {
                session_expiry_secs: se,
                receive_maximum: rm,
                maximum_packet_size: mps,
                topic_alias_maximum: tam,
                request_response_information: rri,
                request_problem_information: rpi,
                // Data only with a method (§3.1.2.11.10).
                authentication_data: am.as_ref().and(ad),
                authentication_method: am,
                user,
            }
        });
    let will = proptest::option::of((
        topic(),
        binary(),
        qos(),
        any::<bool>(),
        (
            any::<u32>(),
            any::<bool>(),
            opt(v5, any::<u32>()),
            opt(v5, text()),
        ),
        (opt(v5, topic()), opt(v5, binary()), user(v5)),
    ))
    .prop_map(move |w| {
        w.map(
            |(topic, payload, qos, retain, (d, pf, me, ct), (rt, cd, user))| Will {
                topic,
                payload,
                qos,
                retain,
                properties: if v5 {
                    WillProperties {
                        delay_secs: d,
                        payload_format_utf8: pf,
                        message_expiry_secs: me,
                        content_type: ct,
                        response_topic: rt,
                        correlation_data: cd,
                        user,
                    }
                } else {
                    WillProperties::default()
                },
            },
        )
    });
    (
        "[a-zA-Z0-9]{0,23}",
        any::<bool>(),
        any::<u16>(),
        will,
        proptest::option::of(text()),
        proptest::option::of(binary()),
        properties,
    )
        .prop_map(
            move |(client, clean_start, keep_alive_secs, will, username, password, properties)| {
                // 3.1.1: no password without a user name (§3.1.2.9).
                let password = if v5 || username.is_some() {
                    password
                } else {
                    None
                };
                Packet::Connect(Box::new(Connect {
                    version: v,
                    client_id: ClientId::new(Bytes::from(client)).unwrap(),
                    clean_start,
                    keep_alive_secs,
                    will,
                    username,
                    password,
                    properties,
                }))
            },
        )
}

fn connack(v: Version) -> impl Strategy<Value = Packet> {
    let v5 = v == Version::V5;
    let reasons = if v5 {
        vec![
            ReasonCode::Success,
            ReasonCode::NotAuthorized,
            ReasonCode::ServerBusy,
            ReasonCode::QosNotSupported,
            ReasonCode::UseAnotherServer,
        ]
    } else {
        vec![
            ReasonCode::Success,
            ReasonCode::UnsupportedProtocolVersion,
            ReasonCode::ClientIdentifierNotValid,
            ReasonCode::ServerUnavailable,
            ReasonCode::BadUserNameOrPassword,
            ReasonCode::NotAuthorized,
        ]
    };
    let properties = (
        (
            opt(v5, any::<u32>()),
            opt(v5, any::<NonZeroU16>()),
            opt(
                v5,
                prop_oneof![Just(Qos::AtMostOnce), Just(Qos::AtLeastOnce)],
            ),
            opt(v5, any::<bool>()),
        ),
        (
            opt(v5, any::<NonZeroU32>()),
            opt(v5, "[a-z0-9]{1,8}"),
            opt(v5, any::<u16>()),
            opt(v5, text()),
        ),
        (
            user(v5),
            opt(v5, any::<bool>()),
            opt(v5, any::<bool>()),
            opt(v5, any::<bool>()),
        ),
        (
            opt(v5, any::<u16>()),
            opt(v5, text()),
            opt(v5, text()),
            opt(v5, text()),
            opt(v5, binary()),
        ),
    )
        .prop_map(
            |(
                (se, rm, mq, ra),
                (mps, aci, tam, rs),
                (user, wa, sia, sa),
                (ska, ri, sr, am, ad),
            )| {
                ConnAckProperties {
                    session_expiry_secs: se,
                    receive_maximum: rm,
                    maximum_qos: mq,
                    retain_available: ra,
                    maximum_packet_size: mps,
                    assigned_client_id: aci.map(|s| ClientId::new(Bytes::from(s)).unwrap()),
                    topic_alias_maximum: tam,
                    reason_string: rs,
                    user,
                    wildcard_subscription_available: wa,
                    subscription_identifiers_available: sia,
                    shared_subscription_available: sa,
                    server_keep_alive: ska,
                    response_information: ri,
                    server_reference: sr,
                    authentication_method: am,
                    authentication_data: ad,
                }
            },
        );
    (proptest::sample::select(reasons), any::<bool>(), properties).prop_map(
        |(reason, present, properties)| {
            Packet::ConnAck(Box::new(ConnAck {
                session_present: present && reason == ReasonCode::Success,
                reason,
                properties,
            }))
        },
    )
}

fn publish(v: Version, from: Sender) -> impl Strategy<Value = Packet> {
    let v5 = v == Version::V5;
    let ids = if v5 && from == Sender::Server { 3 } else { 0 };
    let properties = opt(
        v5,
        (
            (
                any::<bool>(),
                proptest::option::of(any::<u32>()),
                proptest::option::of(any::<NonZeroU16>()),
            ),
            (
                proptest::option::of(topic()),
                proptest::option::of(binary()),
                user(true),
            ),
            (
                proptest::collection::vec(1u32..=268_435_455, 0..=ids),
                proptest::option::of(text()),
            ),
        )
            .prop_map(
                |((pf, me, ta), (rt, cd, user), (ids, ct))| PublishProperties {
                    payload_format_utf8: pf,
                    message_expiry_secs: me,
                    topic_alias: ta,
                    response_topic: rt,
                    correlation_data: cd,
                    user,
                    subscription_identifiers: ids
                        .into_iter()
                        .map(|i| NonZeroU32::new(i).unwrap())
                        .collect(),
                    content_type: ct,
                },
            ),
    );
    (
        proptest::option::of(topic()),
        qos(),
        id(),
        any::<bool>(),
        any::<bool>(),
        binary(),
        properties,
    )
        .prop_map(move |(topic, qos, id, dup, retain, payload, props)| {
            // No properties is `None`, not a box of defaults.
            let props = props.filter(|p| *p != PublishProperties::default());
            let aliased = props.as_ref().is_some_and(|p| p.topic_alias.is_some());
            // Only an alias may stand in for the topic.
            let topic = if aliased {
                topic
            } else {
                topic.or_else(|| Some(Topic::new(Bytes::from_static(b"t")).unwrap()))
            };
            let id = (qos != Qos::AtMostOnce).then_some(id);
            Packet::Publish(Publish {
                topic,
                id,
                qos,
                duplicate: dup && qos != Qos::AtMostOnce,
                retain,
                payload,
                properties: props.map(Box::new),
            })
        })
}

fn ack_properties(v5: bool) -> impl Strategy<Value = AckProperties> {
    (opt(v5, text()), user(v5)).prop_map(|(reason_string, user)| AckProperties {
        reason_string,
        user,
    })
}

fn puback(v: Version) -> impl Strategy<Value = Packet> {
    let v5 = v == Version::V5;
    let reasons = if v5 {
        vec![
            ReasonCode::Success,
            ReasonCode::NoMatchingSubscribers,
            ReasonCode::QuotaExceeded,
            ReasonCode::NotAuthorized,
        ]
    } else {
        vec![ReasonCode::Success]
    };
    (id(), proptest::sample::select(reasons), ack_properties(v5)).prop_map(
        |(id, reason, properties)| {
            Packet::PubAck(PubAck {
                id,
                reason,
                properties,
            })
        },
    )
}

fn subscribe(v: Version) -> impl Strategy<Value = Packet> {
    let v5 = v == Version::V5;
    let options =
        (qos(), any::<bool>(), any::<bool>(), 0u8..3).prop_map(move |(qos, nl, rap, rh)| {
            if !v5 {
                return SubscriptionOptions::qos(qos);
            }
            SubscriptionOptions {
                qos,
                no_local: nl,
                retain_as_published: rap,
                retain_handling: match rh {
                    0 => RetainHandling::OnSubscribe,
                    1 => RetainHandling::OnNewSubscription,
                    _ => RetainHandling::Never,
                },
            }
        });
    (
        id(),
        opt(
            v5,
            any::<NonZeroU32>().prop_map(|v| NonZeroU32::new(v.get() % 268_435_455 + 1).unwrap()),
        ),
        user(v5),
        proptest::collection::vec((filter(), options), 1..4),
    )
        .prop_map(|(id, subscription_identifier, user, filters)| {
            Packet::Subscribe(Subscribe {
                id,
                subscription_identifier,
                user,
                filters,
            })
        })
}

fn suback(v: Version) -> impl Strategy<Value = Packet> {
    let v5 = v == Version::V5;
    let reasons = if v5 {
        vec![
            ReasonCode::Success,
            ReasonCode::GrantedQos1,
            ReasonCode::GrantedQos2,
            ReasonCode::NotAuthorized,
            ReasonCode::TopicFilterInvalid,
        ]
    } else {
        vec![
            ReasonCode::Success,
            ReasonCode::GrantedQos1,
            ReasonCode::GrantedQos2,
            ReasonCode::UnspecifiedError,
        ]
    };
    (
        id(),
        proptest::collection::vec(proptest::sample::select(reasons), 1..4),
        ack_properties(v5),
    )
        .prop_map(|(id, reasons, properties)| {
            Packet::SubAck(SubAck {
                id,
                reasons,
                properties,
            })
        })
}

fn unsubscribe(v: Version) -> impl Strategy<Value = Packet> {
    (
        id(),
        user(v == Version::V5),
        proptest::collection::vec(filter(), 1..4),
    )
        .prop_map(|(id, user, filters)| Packet::Unsubscribe(Unsubscribe { id, user, filters }))
}

fn unsuback(v: Version) -> impl Strategy<Value = Packet> {
    let v5 = v == Version::V5;
    let count = if v5 { 1..4 } else { 0..1 };
    (
        id(),
        proptest::collection::vec(
            proptest::sample::select(vec![
                ReasonCode::Success,
                ReasonCode::NoSubscriptionExisted,
                ReasonCode::NotAuthorized,
            ]),
            count,
        ),
        ack_properties(v5),
    )
        .prop_map(|(id, reasons, properties)| {
            Packet::UnsubAck(UnsubAck {
                id,
                reasons,
                properties,
            })
        })
}

fn disconnect(v: Version, from: Sender) -> impl Strategy<Value = Packet> {
    let v5 = v == Version::V5;
    let mut reasons = vec![ReasonCode::Success];
    if v5 {
        reasons.extend([
            ReasonCode::ServerBusy,
            ReasonCode::ProtocolError,
            ReasonCode::QuotaExceeded,
        ]);
        if from == Sender::Client {
            reasons.push(ReasonCode::DisconnectWithWill);
        }
    }
    let client = from == Sender::Client;
    (
        proptest::sample::select(reasons),
        opt(v5 && client, any::<u32>()),
        opt(v5, text()),
        user(v5),
        opt(v5 && !client, text()),
    )
        .prop_map(|(reason, se, rs, user, sr)| {
            Packet::Disconnect(Disconnect {
                reason,
                properties: DisconnectProperties {
                    session_expiry_secs: se,
                    reason_string: rs,
                    user,
                    server_reference: sr,
                },
            })
        })
}

fn auth() -> impl Strategy<Value = Packet> {
    (
        proptest::sample::select(vec![
            ReasonCode::Success,
            ReasonCode::ContinueAuthentication,
            ReasonCode::ReAuthenticate,
        ]),
        proptest::option::of(text()),
        proptest::option::of(binary()),
        proptest::option::of(text()),
        user(true),
    )
        .prop_map(|(reason, am, ad, rs, user)| {
            Packet::Auth(Auth {
                reason,
                properties: AuthProperties {
                    authentication_method: am,
                    authentication_data: ad,
                    reason_string: rs,
                    user,
                },
            })
        })
}

/// Every packet `from` may send in `v`.
fn packet(v: Version, from: Sender) -> BoxedStrategy<Packet> {
    let v5 = v == Version::V5;
    let mut options: Vec<BoxedStrategy<Packet>> = vec![publish(v, from).boxed(), puback(v).boxed()];
    match from {
        Sender::Client => {
            options.push(connect(v).boxed());
            options.push(subscribe(v).boxed());
            options.push(unsubscribe(v).boxed());
            options.push(Just(Packet::PingReq).boxed());
            options.push(disconnect(v, from).boxed());
        }
        Sender::Server => {
            options.push(connack(v).boxed());
            options.push(suback(v).boxed());
            options.push(unsuback(v).boxed());
            options.push(Just(Packet::PingResp).boxed());
            if v5 {
                options.push(disconnect(v, from).boxed());
            }
        }
    }
    if v5 {
        options.push(auth().boxed());
    }
    proptest::strategy::Union::new(options).boxed()
}

fn case() -> impl Strategy<Value = (Version, Sender, Packet)> {
    (
        version(),
        prop_oneof![Just(Sender::Client), Just(Sender::Server)],
    )
        .prop_flat_map(|(v, from)| packet(v, from).prop_map(move |p| (v, from, p)))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    #[test]
    fn every_packet_survives_encode_and_decode((version, from, packet) in case()) {
        let mut buf = BytesMut::new();
        encode(&packet, version, from, &mut buf).unwrap();
        let connecting = matches!(packet, Packet::Connect(_));
        let known = (!connecting).then_some(version);
        let decoded = decode(&mut buf, known, from, &Limits::default()).unwrap();
        prop_assert_eq!(decoded, Some(packet));
        prop_assert!(buf.is_empty(), "the decoder took exactly the packet");
    }

    /// Arbitrary bytes: an error or a packet, never a panic, never more than
    /// the buffer held.
    #[test]
    fn arbitrary_bytes_never_panic(
        bytes in proptest::collection::vec(any::<u8>(), 0..300),
        version in proptest::option::of(version()),
        server in any::<bool>(),
    ) {
        let from = if server { Sender::Server } else { Sender::Client };
        let mut buf = BytesMut::from(&bytes[..]);
        while let Ok(Some(_)) = decode(&mut buf, version, from, &Limits::default()) {}
        prop_assert!(buf.len() <= bytes.len());
    }

    /// A packet arriving a byte at a time decodes to the same packet, and
    /// nothing is consumed until it is whole.
    #[test]
    fn a_packet_split_anywhere_decodes_whole((version, from, packet) in case(), cut in any::<prop::sample::Index>()) {
        let mut whole = BytesMut::new();
        encode(&packet, version, from, &mut whole).unwrap();
        let at = cut.index(whole.len());
        let connecting = matches!(packet, Packet::Connect(_));
        let known = (!connecting).then_some(version);
        let mut buf = BytesMut::from(&whole[..at]);
        prop_assert_eq!(decode(&mut buf, known, from, &Limits::default()).unwrap(), None);
        prop_assert_eq!(buf.len(), at);
        buf.extend_from_slice(&whole[at..]);
        prop_assert_eq!(decode(&mut buf, known, from, &Limits::default()).unwrap(), Some(packet));
    }
}
