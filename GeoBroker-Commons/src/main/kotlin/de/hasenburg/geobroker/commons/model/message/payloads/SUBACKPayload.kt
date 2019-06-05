package de.hasenburg.geobroker.commons.model.message.payloads

import de.hasenburg.geobroker.commons.model.message.ReasonCode

data class SUBACKPayload(val reasonCode: ReasonCode) : AbstractPayload()
