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
import org.apache.logging.log4j.LogManager
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test

class PublishSubscribeTest {

    private val logger = LogManager.getLogger()
    private lateinit var serverLogic: SingleGeoBrokerServerLogic
    private lateinit var clientProcessManager: ZMQProcessManager

    @Before
    fun setUp() {
        logger.info("Running test setUp")

        serverLogic = SingleGeoBrokerServerLogic()
        serverLogic.loadConfiguration(Configuration())
        serverLogic.initializeFields()
        serverLogic.startServer()

        clientProcessManager = ZMQProcessManager()
    }

    @After
    fun tearDown() {
        logger.info("Running test tearDown.")
        clientProcessManager.tearDown(2000)
        serverLogic.cleanUp()
    }

    @Test
    fun testSubscribeInGeofence() {
        logger.info("RUNNING testSubscribeInGeofence TEST")

        // connect, ping, and disconnect
        val l = Location.random()
        val g = Geofence.circle(l, 0.4)
        val t = Topic("test")

        val client = SimpleClient("localhost", 5559, clientProcessManager)
        client.send(CONNECTPayload(l))
        client.send(SUBSCRIBEPayload(t, g))
        client.send(PUBLISHPayload(t, g, "Content"))

        sleepNoLog(500, 0)

        // validate received payloads
        assertTrue(client.receiveWithTimeout(100) is CONNACKPayload)
        assertTrue(client.receiveWithTimeout(100) is SUBACKPayload)
        var payload = client.receiveWithTimeout(100)
        logger.info("Received published message {}", payload)
        if (payload is PUBLISHPayload) {
            assertEquals("Content", payload.content)
        } else {
            fail("Wrong payload, received $payload")
        }

        payload = client.receiveWithTimeout(100)
        if (payload is PUBACKPayload) {
            assertEquals(ReasonCode.Success, payload.reasonCode)
        } else {
            fail("Wrong payload, received $payload")
        }
    }

    @Test
    fun testSubscriberNotInGeofence() {
        // subscriber
        val l = Location.random()
        val g = Geofence.circle(l, 0.4)
        val t = Topic("test")

        val clientSubscriber = SimpleClient("localhost", 5559, clientProcessManager)
        clientSubscriber.send(CONNECTPayload(Location.undefined())) // subscriber not in geofence
        clientSubscriber.send(SUBSCRIBEPayload(t, g))

        // publisher
        val clientPublisher = SimpleClient("localhost", 5559, clientProcessManager)
        clientPublisher.send(CONNECTPayload(l)) // publisher is in geofence
        clientPublisher.send(PUBLISHPayload(t, g, "Content"))

        sleepNoLog(500, 0)

        validateNoPublishReceived(clientSubscriber, clientPublisher)
    }

    @Test
    fun testPublisherNotInGeofence() {
        // subscriber
        val l = Location.random()
        val g = Geofence.circle(l, 0.4)
        val t = Topic("test")

        val clientSubscriber = SimpleClient("localhost", 5559, clientProcessManager)
        clientSubscriber.send(CONNECTPayload(l)) // subscriber is in geofence
        clientSubscriber.send(SUBSCRIBEPayload(t, g))

        // publisher
        val clientPublisher = SimpleClient( "localhost", 5559, clientProcessManager)
        clientPublisher.send(CONNECTPayload(Location.undefined())) // publisher is not in geofence
        clientPublisher.send(PUBLISHPayload(t, g, "Content"))

        sleepNoLog(500, 0)

        validateNoPublishReceived(clientSubscriber, clientPublisher)
    }

    private fun validateNoPublishReceived(clientSubscriber: SimpleClient, clientPublisher: SimpleClient) {
        // check subscriber messages: must not contain "PUBLISH"
        val subscriberMessageCount = 2
        for (i in 0 until subscriberMessageCount) {
            assertFalse(clientSubscriber.receiveWithTimeout(100) is PUBLISHPayload)
        }

        // check publisher messages: second should be a PUBACK with no matching subscribers
        assertFalse(clientPublisher.receiveWithTimeout(100) is PUBLISHPayload)

        val payload = clientPublisher.receiveWithTimeout(100)
        if (payload is PUBACKPayload) {
            assertEquals(ReasonCode.NoMatchingSubscribers, payload.reasonCode)
        } else {
            fail("Wrong payload, received $payload")
        }

    }

}
