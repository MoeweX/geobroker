package de.hasenburg.geobroker.commons.model.message.payloads

import de.hasenburg.geobroker.commons.model.message.Topic
import de.hasenburg.geobroker.commons.model.spatial.Geofence

import java.util.Objects

data class PUBLISHPayload(val topic: Topic, val geofence: Geofence, val content: String) : AbstractPayload()
