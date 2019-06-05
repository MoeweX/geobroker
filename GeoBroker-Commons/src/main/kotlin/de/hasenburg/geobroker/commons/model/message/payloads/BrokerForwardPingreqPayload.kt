package de.hasenburg.geobroker.commons.model.message.payloads

data class BrokerForwardPingreqPayload(val clientIdentifier: String, val pingreqPayload: PINGREQPayload) :
    AbstractPayload()
