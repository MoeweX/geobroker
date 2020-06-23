package de.hasenburg.geobroker.server.scenarios

import de.hasenburg.geobroker.client.main.SimpleClient
import de.hasenburg.geobroker.commons.*
import de.hasenburg.geobroker.commons.communication.ZMQProcessManager
import de.hasenburg.geobroker.commons.model.message.Payload.*
import de.hasenburg.geobroker.commons.model.message.ReasonCode
import de.hasenburg.geobroker.commons.model.message.Topic
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.server.main.Configuration
import de.hasenburg.geobroker.server.main.server.SingleGeoBrokerServerLogic
import io.prometheus.client.CollectorRegistry
import org.apache.logging.log4j.LogManager
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test

class SubscribeUnsubscribeTest {

    private val logger = LogManager.getLogger()
    private lateinit var serverLogic: SingleGeoBrokerServerLogic
    private lateinit var client: SimpleClient

    @Before
    fun setUp() {
        logger.info("Running test setUp")
        CollectorRegistry.defaultRegistry.clear();

        serverLogic = SingleGeoBrokerServerLogic()
        serverLogic.loadConfiguration(Configuration())
        serverLogic.initializeFields()
        serverLogic.startServer()

    }

    @After
    fun tearDown() {
        logger.info("Running test tearDown.")
        client.tearDownClient()
        serverLogic.cleanUp()
    }

    @Test
    fun testSubscribeUnsubscribe() {
        // connect, ping, and disconnect
        val cI = "testClient"
        val l = Location.random()
        val g = Geofence.circle(l, 0.4)
        val t = Topic("test")

        client = SimpleClient("localhost", 5559, identity = cI)
        client.send(CONNECTPayload(l))
        client.send(SUBSCRIBEPayload(t, g))

        sleepNoLog(500, 0)

        // validate payloads
        assertTrue(client.receiveWithTimeout(100) is CONNACKPayload)
        assertTrue(client.receiveWithTimeout(100) is SUBACKPayload)
        assertEquals(1, serverLogic.clientDirectory.getCurrentClientSubscriptions(cI).toLong())
        logger.info("Client has successfully subscribed")

        // Unsubscribe
        client.send(UNSUBSCRIBEPayload(t))

        val payload = client.receiveWithTimeout(100)
        if (payload is UNSUBACKPayload) {
            assertEquals(ReasonCode.Success, payload.reasonCode)
        } else {
            fail("Wrong payload, received $payload")
        }
        assertEquals(0, serverLogic.clientDirectory.getCurrentClientSubscriptions(cI).toLong())
    }

    @Test
    fun testUnsubscribeNotConnected() {
        // connect, ping, and disconnect
        val t = Topic("test")
        val cI = "testClient"

        client = SimpleClient("localhost", 5559, identity = cI)

        // Unsubscribe
        client.send(UNSUBSCRIBEPayload(t))

        val payload = client.receiveWithTimeout(100)
        if (payload is UNSUBACKPayload) {
            assertEquals(ReasonCode.NoSubscriptionExisted, payload.reasonCode)
        } else {
            fail("Wrong payload, received $payload")
        }
        assertEquals(0, serverLogic.clientDirectory.getCurrentClientSubscriptions(cI).toLong())
    }

}
