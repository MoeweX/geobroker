package de.hasenburg.geobroker.commons.model.message

import de.hasenburg.geobroker.commons.*
import de.hasenburg.geobroker.commons.model.KryoSerializer
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import org.apache.logging.log4j.LogManager
import org.junit.Test
import org.zeromq.ZMsg

import java.util.Arrays

import de.hasenburg.geobroker.commons.model.message.Payload.*
import org.junit.Assert.*

class PayloadTest {

    private val logger = LogManager.getLogger()
    private val kryo = KryoSerializer()

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


    @Test
    fun testBrokerForwardPublishPayload() {
        val publisherMatchingPayload = BrokerForwardPublishPayload(PUBLISHPayload(Topic("data"),
                Geofence.circle(Location.random(), 1.0),
                "Some random content"), subscriberClientIdentifiers = listOf("Subscriber 1", "Subscriber 2"))
        transformAndCheck(publisherMatchingPayload)

        val subscriberMatchingPayload = BrokerForwardPublishPayload(PUBLISHPayload(Topic("data"),
                Geofence.circle(Location.random(), 1.0),
                "Some random content"), Location.undefined())
        transformAndCheck(subscriberMatchingPayload)
    }

    private fun transformAndCheck(payload: Payload) {
        val message = payloadToZMsg(payload, kryo)
        val payload2 = message.transformZMsg(kryo)
        logger.info(payload2)
        assertEquals(payload, payload2)
    }

}
