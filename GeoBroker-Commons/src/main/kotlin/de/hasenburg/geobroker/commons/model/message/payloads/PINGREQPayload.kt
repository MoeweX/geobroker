package de.hasenburg.geobroker.commons.model.message.payloads

import de.hasenburg.geobroker.commons.model.spatial.Location

data class PINGREQPayload(val location: Location) : AbstractPayload()
