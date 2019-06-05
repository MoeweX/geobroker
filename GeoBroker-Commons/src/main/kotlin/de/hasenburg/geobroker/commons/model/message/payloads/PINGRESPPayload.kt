package de.hasenburg.geobroker.commons.model.message.payloads

import de.hasenburg.geobroker.commons.model.message.ReasonCode

import java.util.Objects

data class PINGRESPPayload(val reasonCode: ReasonCode) : AbstractPayload()
