package de.hasenburg.geobroker.commons.model.message.payloads

import de.hasenburg.geobroker.commons.model.message.Topic
import de.hasenburg.geobroker.commons.model.spatial.Geofence

data class SUBSCRIBEPayload(val topic: Topic, val geofence: Geofence) : AbstractPayload()
