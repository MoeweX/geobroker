package de.hasenburg.geobroker.commons.model.message.payloads

import de.hasenburg.geobroker.commons.model.message.Topic

data class UNSUBSCRIBEPayload(val topic: Topic) : AbstractPayload()
