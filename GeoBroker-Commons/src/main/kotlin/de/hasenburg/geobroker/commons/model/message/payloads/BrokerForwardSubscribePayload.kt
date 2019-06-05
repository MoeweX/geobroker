package de.hasenburg.geobroker.commons.model.message.payloads

data class BrokerForwardSubscribePayload(val clientIdentifier: String, val subscribePayload: SUBSCRIBEPayload) :
    AbstractPayload()
