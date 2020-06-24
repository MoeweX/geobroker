package de.hasenburg.geobroker.commons.model.message

import de.hasenburg.geobroker.commons.*
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import org.apache.logging.log4j.LogManager
import org.junit.Test

import java.util.Arrays

import de.hasenburg.geobroker.commons.model.message.Payload.*
import org.junit.Assert.*

class PayloadTest {

    private val logger = LogManager.getLogger()

    @Test
    fun testCONNECTPayload() {
        val payload = CONNECTPayload(Location.random())
        transformAndCheck(payload)
    }

    @Test
    fun testPINGREQPayload() {
        val payload = PINGREQPayload(Location.random())
        transformAndCheck(payload)
    }

    @Test
    fun testPUBLISHPayload() {
        val payload = PUBLISHPayload(Topic("data"),
                Geofence.circle(Location.random(), 1.0),
                generatePayloadWithSize(20, "test-"))
        transformAndCheck(payload)
    }

    @Test
    fun testSUBSCRIBEPayload() {
        val payload = SUBSCRIBEPayload(Topic("data"), Geofence.circle(Location.random(), 1.0))
        transformAndCheck(payload)
    }

    private fun transformAndCheck(payload: Payload) {
        val message = payload.toZMsg()
        val payload2 = message.toPayload()
        logger.info(payload2)
        assertEquals(payload, payload2)
    }

}
