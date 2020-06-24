package de.hasenburg.geobroker.commons.model.message

import kotlinx.serialization.Serializable

@Serializable
enum class ReasonCode {
    NormalDisconnection, //
    ProtocolError, //
    NotConnectedOrNoLocation, //
    GrantedQoS0, //
    Success, //
    NoMatchingSubscribers, //
    NoSubscriptionExisted, //

    // New Reason Codes
    LocationUpdated, //
}
