package de.hasenburg.geobroker.commons.model.message.payloads

import de.hasenburg.geobroker.commons.model.message.ReasonCode

class CONNACKPayload(val reasonCode: ReasonCode) : AbstractPayload()