package de.hasenburg.geobroker.commons.model.message.payloads

data class BrokerForwardDisconnectPayload(val clientIdentifier: String, val disconnectPayload: DISCONNECTPayload) :
    AbstractPayload()
