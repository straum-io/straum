//! Reason codes (MQTT 5 §2.4), and what 3.1.1 can say of them.

/// Every reason code MQTT 5 defines. A byte that is none of them is refused
/// when decoding: a receiver cannot act on a reason it does not know.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ReasonCode {
    /// Success, Normal disconnection, Granted QoS 0: one byte, three names.
    Success = 0x00,
    GrantedQos1 = 0x01,
    GrantedQos2 = 0x02,
    DisconnectWithWill = 0x04,
    NoMatchingSubscribers = 0x10,
    NoSubscriptionExisted = 0x11,
    ContinueAuthentication = 0x18,
    ReAuthenticate = 0x19,
    UnspecifiedError = 0x80,
    MalformedPacket = 0x81,
    ProtocolError = 0x82,
    ImplementationSpecificError = 0x83,
    UnsupportedProtocolVersion = 0x84,
    ClientIdentifierNotValid = 0x85,
    BadUserNameOrPassword = 0x86,
    NotAuthorized = 0x87,
    ServerUnavailable = 0x88,
    ServerBusy = 0x89,
    Banned = 0x8A,
    ServerShuttingDown = 0x8B,
    BadAuthenticationMethod = 0x8C,
    KeepAliveTimeout = 0x8D,
    SessionTakenOver = 0x8E,
    TopicFilterInvalid = 0x8F,
    TopicNameInvalid = 0x90,
    PacketIdentifierInUse = 0x91,
    PacketIdentifierNotFound = 0x92,
    ReceiveMaximumExceeded = 0x93,
    TopicAliasInvalid = 0x94,
    PacketTooLarge = 0x95,
    MessageRateTooHigh = 0x96,
    QuotaExceeded = 0x97,
    AdministrativeAction = 0x98,
    PayloadFormatInvalid = 0x99,
    RetainNotSupported = 0x9A,
    QosNotSupported = 0x9B,
    UseAnotherServer = 0x9C,
    ServerMoved = 0x9D,
    SharedSubscriptionsNotSupported = 0x9E,
    ConnectionRateExceeded = 0x9F,
    MaximumConnectTime = 0xA0,
    SubscriptionIdentifiersNotSupported = 0xA1,
    WildcardSubscriptionsNotSupported = 0xA2,
}

impl ReasonCode {
    pub fn from_u8(value: u8) -> Option<Self> {
        use ReasonCode::*;
        Some(match value {
            0x00 => Success,
            0x01 => GrantedQos1,
            0x02 => GrantedQos2,
            0x04 => DisconnectWithWill,
            0x10 => NoMatchingSubscribers,
            0x11 => NoSubscriptionExisted,
            0x18 => ContinueAuthentication,
            0x19 => ReAuthenticate,
            0x80 => UnspecifiedError,
            0x81 => MalformedPacket,
            0x82 => ProtocolError,
            0x83 => ImplementationSpecificError,
            0x84 => UnsupportedProtocolVersion,
            0x85 => ClientIdentifierNotValid,
            0x86 => BadUserNameOrPassword,
            0x87 => NotAuthorized,
            0x88 => ServerUnavailable,
            0x89 => ServerBusy,
            0x8A => Banned,
            0x8B => ServerShuttingDown,
            0x8C => BadAuthenticationMethod,
            0x8D => KeepAliveTimeout,
            0x8E => SessionTakenOver,
            0x8F => TopicFilterInvalid,
            0x90 => TopicNameInvalid,
            0x91 => PacketIdentifierInUse,
            0x92 => PacketIdentifierNotFound,
            0x93 => ReceiveMaximumExceeded,
            0x94 => TopicAliasInvalid,
            0x95 => PacketTooLarge,
            0x96 => MessageRateTooHigh,
            0x97 => QuotaExceeded,
            0x98 => AdministrativeAction,
            0x99 => PayloadFormatInvalid,
            0x9A => RetainNotSupported,
            0x9B => QosNotSupported,
            0x9C => UseAnotherServer,
            0x9D => ServerMoved,
            0x9E => SharedSubscriptionsNotSupported,
            0x9F => ConnectionRateExceeded,
            0xA0 => MaximumConnectTime,
            0xA1 => SubscriptionIdentifiersNotSupported,
            0xA2 => WildcardSubscriptionsNotSupported,
            _ => return None,
        })
    }

    pub fn code(self) -> u8 {
        self as u8
    }

    /// 0x80 and above are failures (MQTT 5 §2.4).
    pub fn is_failure(self) -> bool {
        self.code() >= 0x80
    }

    /// The CONNACK return code 3.1.1 has for this reason (§3.2.2.3). It has
    /// five refusals; every other failure becomes "server unavailable",
    /// which promises the client nothing it could act on wrongly.
    pub(crate) fn connack_v311(self) -> u8 {
        match self {
            Self::Success => 0x00,
            Self::UnsupportedProtocolVersion => 0x01,
            Self::ClientIdentifierNotValid => 0x02,
            Self::BadUserNameOrPassword => 0x04,
            Self::NotAuthorized => 0x05,
            _ => 0x03,
        }
    }

    pub(crate) fn from_connack_v311(code: u8) -> Option<Self> {
        Some(match code {
            0x00 => Self::Success,
            0x01 => Self::UnsupportedProtocolVersion,
            0x02 => Self::ClientIdentifierNotValid,
            0x03 => Self::ServerUnavailable,
            0x04 => Self::BadUserNameOrPassword,
            0x05 => Self::NotAuthorized,
            _ => return None,
        })
    }

    /// The SUBACK return code 3.1.1 has for this reason: a granted QoS or
    /// 0x80, failure (§3.9.3).
    pub(crate) fn suback_v311(self) -> u8 {
        match self {
            Self::Success => 0x00,
            Self::GrantedQos1 => 0x01,
            Self::GrantedQos2 => 0x02,
            _ => 0x80,
        }
    }
}

/// Which reason codes each packet may carry (MQTT 5 §3.x.2.1).
pub(crate) mod allowed {
    use super::ReasonCode::{self, *};

    pub const CONNACK: &[ReasonCode] = &[
        Success,
        UnspecifiedError,
        MalformedPacket,
        ProtocolError,
        ImplementationSpecificError,
        UnsupportedProtocolVersion,
        ClientIdentifierNotValid,
        BadUserNameOrPassword,
        NotAuthorized,
        ServerUnavailable,
        ServerBusy,
        Banned,
        BadAuthenticationMethod,
        TopicNameInvalid,
        PacketTooLarge,
        QuotaExceeded,
        PayloadFormatInvalid,
        RetainNotSupported,
        QosNotSupported,
        UseAnotherServer,
        ServerMoved,
        ConnectionRateExceeded,
    ];
    pub const PUBACK: &[ReasonCode] = &[
        Success,
        NoMatchingSubscribers,
        UnspecifiedError,
        ImplementationSpecificError,
        NotAuthorized,
        TopicNameInvalid,
        PacketIdentifierInUse,
        QuotaExceeded,
        PayloadFormatInvalid,
    ];
    pub const SUBACK: &[ReasonCode] = &[
        Success,
        GrantedQos1,
        GrantedQos2,
        UnspecifiedError,
        ImplementationSpecificError,
        NotAuthorized,
        TopicFilterInvalid,
        PacketIdentifierInUse,
        QuotaExceeded,
        SharedSubscriptionsNotSupported,
        SubscriptionIdentifiersNotSupported,
        WildcardSubscriptionsNotSupported,
    ];
    pub const UNSUBACK: &[ReasonCode] = &[
        Success,
        NoSubscriptionExisted,
        UnspecifiedError,
        ImplementationSpecificError,
        NotAuthorized,
        TopicFilterInvalid,
        PacketIdentifierInUse,
    ];
    pub const DISCONNECT: &[ReasonCode] = &[
        Success,
        DisconnectWithWill,
        UnspecifiedError,
        MalformedPacket,
        ProtocolError,
        ImplementationSpecificError,
        NotAuthorized,
        ServerBusy,
        ServerShuttingDown,
        KeepAliveTimeout,
        SessionTakenOver,
        TopicFilterInvalid,
        TopicNameInvalid,
        ReceiveMaximumExceeded,
        TopicAliasInvalid,
        PacketTooLarge,
        MessageRateTooHigh,
        QuotaExceeded,
        AdministrativeAction,
        PayloadFormatInvalid,
        RetainNotSupported,
        QosNotSupported,
        UseAnotherServer,
        ServerMoved,
        SharedSubscriptionsNotSupported,
        ConnectionRateExceeded,
        MaximumConnectTime,
        SubscriptionIdentifiersNotSupported,
        WildcardSubscriptionsNotSupported,
    ];
    pub const AUTH: &[ReasonCode] = &[Success, ContinueAuthentication, ReAuthenticate];
}
