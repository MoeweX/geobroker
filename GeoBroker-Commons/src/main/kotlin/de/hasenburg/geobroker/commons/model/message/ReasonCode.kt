package de.hasenburg.geobroker.commons.model.message

enum class ReasonCode {
    NormalDisconnection, //
    ProtocolError, //
    NotConnected, //
    GrantedQoS0, //
    Success, //
    NoMatchingSubscribers, //
    NoSubscriptionExisted, //

    // New Reason Codes
    LocationUpdated, //
    WrongBroker, //
    NoMatchingSubscribersButForwarded //  locally there are no subscribers, but others MIGHT have some
}
