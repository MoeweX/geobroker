package de.hasenburg.geobroker.commons.model.message.payloads

import de.hasenburg.geobroker.commons.model.spatial.Location

data class CONNECTPayload(val location: Location) : AbstractPayload()

