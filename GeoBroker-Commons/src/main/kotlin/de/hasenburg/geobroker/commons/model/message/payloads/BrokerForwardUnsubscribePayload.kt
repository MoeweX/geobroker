package de.hasenburg.geobroker.commons.model.message.payloads

data class BrokerForwardUnsubscribePayload(val clientIdentifier: String, val unsubscribePayload: UNSUBSCRIBEPayload) :
    AbstractPayload()