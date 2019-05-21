package de.hasenburg.geobroker.server.scenarios.disgb

import de.hasenburg.geobroker.client.communication.InternalClientMessage
import de.hasenburg.geobroker.client.main.SimpleClient
import de.hasenburg.geobroker.commons.communication.ZMQProcessManager
import de.hasenburg.geobroker.commons.model.message.ControlPacketType
import de.hasenburg.geobroker.commons.model.message.ReasonCode
import de.hasenburg.geobroker.commons.model.message.Topic
import de.hasenburg.geobroker.commons.model.message.payloads.CONNECTPayload
import de.hasenburg.geobroker.commons.model.message.payloads.PINGREQPayload
import de.hasenburg.geobroker.commons.model.message.payloads.PUBLISHPayload
import de.hasenburg.geobroker.commons.model.message.payloads.SUBSCRIBEPayload
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.commons.sleepNoLog
import de.hasenburg.geobroker.server.main.Configuration
import de.hasenburg.geobroker.server.main.server.DisGBSubscriberMatchingServerLogic
import org.apache.logging.log4j.LogManager
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test

private var logger = LogManager.getLogger()

class SubscriberMatchingRun {

    private val configurationFile = arrayOf("disgb_scenario-paris.toml", "disgb_scenario-berlin.toml")
    // areas must be compatible to disgb_scenario.json
    private val parisArea = Geofence.circle(Location(48.86, 2.35), 3.0)
    private val berlinArea = Geofence.circle(Location(52.52, 13.4), 3.0)

    private val topics = arrayOf(Topic("data/sub1"), Topic("data/sub2"), Topic("data/sub3"))

    // client locations for experiments
    private val cl_1 = Location(45.87, 2.3)
    private val cl_2 = Location(48.0, 2.0)
    private val cl_3 = Location(52.0, 13.0)

    // geofences for experiments
    private val sg_1 = Geofence.rectangle(Location(46.5, 1.0), Location(54.0, 14.0))
    private val sg_2 = Geofence.circle(Location(48.86, 2.35), 1.5)
    private val sg_3 = Geofence.circle(Location(52.52, 13.4), 1.5)

    private val mg_1 = Geofence.rectangle(Location(46.4, 0.9), Location(54.1, 14.1))
    private val mg_2 = Geofence.circle(Location(48.86, 2.35), 1.6)
    private val mg_3 = Geofence.circle(Location(52.52, 13.4), 1.6)

    // other fields
    private lateinit var paris: DisGBSubscriberMatchingServerLogic
    private lateinit var berlin: DisGBSubscriberMatchingServerLogic

    private lateinit var clientProcessManager: ZMQProcessManager
    private lateinit var clients: List<SimpleClient>

    @Before
    fun setUp() {
        paris = DisGBSubscriberMatchingServerLogic()
        berlin = DisGBSubscriberMatchingServerLogic()

        for ((i, lifecycle) in listOf(paris, berlin).withIndex()) {
            val c = Configuration.readConfiguration(configurationFile[i])
            lifecycle.loadConfiguration(c)
            lifecycle.initializeFields()
            lifecycle.startServer()
            logger.info("Started server ${c.brokerId}")
        }

        clientProcessManager = ZMQProcessManager()
        clients = setupLocalhostClients(listOf(5558, 5558, 5559))

        logger.info("Setup completed")
    }

    @After
    fun tearDown() {
        clientProcessManager.tearDown(1000)
        berlin.cleanUp()
        paris.cleanUp()
        sleepNoLog(500, 0) // give zeromq some time
        logger.info("Teardown completed")
    }

    @Test
    fun setUpTearDown() {
        logger.info("Servers seem to be running")
    }

    @Test
    fun validateVariables() {
        // broker areas
        assertFalse(parisArea.intersects(berlinArea))
        assertTrue(parisArea.contains(cl_1))
        assertTrue(parisArea.contains(cl_2))
        assertTrue(berlinArea.contains(cl_3))

        // check intersection of geofences with broker areas
        assertTrue(parisArea.intersects(mg_1))
        assertTrue(parisArea.intersects(mg_2))
        assertFalse(parisArea.intersects(mg_3))
        assertTrue(berlinArea.intersects(mg_1))
        assertFalse(berlinArea.intersects(mg_2))
        assertTrue(berlinArea.intersects(mg_3))

        assertTrue(parisArea.intersects(sg_1))
        assertTrue(parisArea.intersects(sg_2))
        assertFalse(parisArea.intersects(sg_3))
        assertTrue(berlinArea.intersects(sg_1))
        assertFalse(berlinArea.intersects(sg_2))
        assertTrue(berlinArea.intersects(sg_3))

        // subscription geofences and client locations
        assertFalse(sg_1.contains(cl_1))
        assertTrue(sg_1.contains(cl_2))
        assertTrue(sg_1.contains(cl_3))

        assertFalse(sg_2.contains(cl_1))
        assertTrue(sg_2.contains(cl_2))
        assertFalse(sg_2.contains(cl_3))

        assertFalse(sg_3.contains(cl_1))
        assertFalse(sg_3.contains(cl_2))
        assertTrue(sg_3.contains(cl_3))

        // message geofences and client locations
        assertFalse(mg_1.contains(cl_1))
        assertTrue(mg_1.contains(cl_2))
        assertTrue(mg_1.contains(cl_3))

        assertFalse(mg_2.contains(cl_1))
        assertTrue(mg_2.contains(cl_2))
        assertFalse(mg_2.contains(cl_3))

        assertFalse(mg_3.contains(cl_1))
        assertFalse(mg_3.contains(cl_2))
        assertTrue(mg_3.contains(cl_3))
    }

    @Test
    fun scenario() {
        logger.info("Starting scenario\n\n\n")

        /* ***************************************************************
         * Connect to respective broker
         ****************************************************************/

        // forbidden connect
        sendCONNECT(clients[0],
                Location.randomInGeofence(berlinArea),
                ControlPacketType.DISCONNECT,
                ReasonCode.WrongBroker)

        // correct connect
        sendCONNECT(clients[0], Location.randomInGeofence(parisArea))
        sendCONNECT(clients[1], Location.randomInGeofence(parisArea))
        sendCONNECT(clients[2], Location.randomInGeofence(berlinArea))

        // validate connects internally
        assertEquals(2, paris.clientDirectory.numberOfClients)
        assertEquals(1, berlin.clientDirectory.numberOfClients)

        /* ***************************************************************
         * Update location to what is needed for the experiment
         ****************************************************************/

        sendPINGREQ(clients[0], cl_1)
        sendPINGREQ(clients[1], cl_2)
        sendPINGREQ(clients[2], cl_3)

        // validate whether brokers got locations
        assertEquals(cl_1, paris.clientDirectory.getClientLocation(getClientIdentifier(0)))
        assertEquals(cl_2, paris.clientDirectory.getClientLocation(getClientIdentifier(1)))
        assertEquals(cl_3, berlin.clientDirectory.getClientLocation(getClientIdentifier(2)))

        /* ***************************************************************
         * Create subscriptions
         ****************************************************************/

        // all clients create the same subscriptions
        for (i in 0..2) {
            sendSUBSCRIBE(clients[i], topics[0], sg_1)
            sendSUBSCRIBE(clients[i], topics[1], sg_2)
            sendSUBSCRIBE(clients[i], topics[2], sg_3)
        }

        // validate whether brokers got correct subscriptions (we check only a sample)
        assertNotNull(paris.clientDirectory.getSubscription(getClientIdentifier(0), topics[0]))
        assertNull(berlin.clientDirectory.getSubscription(getClientIdentifier(0), topics[0]))

        /* ***************************************************************
         * Publish message
         ****************************************************************/

        // we need to publish to every topic, as the subscriptions are generated per topic and we want to match against
        // every subscription
        val mgs = arrayOf(mg_1, mg_2, mg_3)
        for (mg in mgs) {
            for (i in 0..2) {
                sendPUBLISH(clients[1], topics[i], mg, generateContent(1, topics[i]))
            }
        }

        // validate for each client the received messages
        validateMessagesForClient1(clients[0])
        // TODO validate for Client2
        validateMessagesForClient3(clients[2])


        sleepNoLog(3000, 0)

        logger.info("Finished scenario\n\n\n")
    }

    fun getClientIdentifier(index: Int): String {
        return "Client-${index + 1}"
    }

    fun generateContent(index: Int, t: Topic) : String {
        return "This message was published by client ${getClientIdentifier(index)} to topic $t"
    }

    fun setupLocalhostClients(ports: List<Int>): List<SimpleClient> {
        val clients = mutableListOf<SimpleClient>()

        for (i in 0..ports.size - 1) {
            val client = SimpleClient(getClientIdentifier(i), "localhost", ports[i], clientProcessManager)
            clients.add(client)
        }

        return clients
    }

    /**
     * Receives all outstanding messages and checks whether they are as expected for Client1.
     */
    fun validateMessagesForClient1(client: SimpleClient) {
        // this client should not have received any messages
        assertNull(client.receiveInternalClientMessageWithTimeout(2000))
    }

    /**
     * Receives all outstanding messages and checks whether they are as expected for Client1.
     */
    fun validateMessagesForClient3(client: SimpleClient) {
        // this client should have received 4 publish messages
        for (i in 1..4) {
            val message = client.receiveInternalClientMessageWithTimeout(500)
            assertEquals(ControlPacketType.PUBLISH, message!!.controlPacketType)
            logger.info(message.payload.publishPayload.get())
            // we could check more fields in here, but we currently do not do this
        }
        assertNull(client.receiveInternalClientMessageWithTimeout(0))
    }

    fun sendCONNECT(client: SimpleClient, l: Location,
                    expectedControlPacketType: ControlPacketType = ControlPacketType.CONNACK,
                    expectedReasonCode: ReasonCode = ReasonCode.Success) {
        client.sendInternalClientMessage(InternalClientMessage(ControlPacketType.CONNECT, CONNECTPayload(l)))
        val internalClientMessage = client.receiveInternalClientMessage()

        assertEquals(expectedControlPacketType, internalClientMessage.controlPacketType)
        when (expectedControlPacketType) {
            ControlPacketType.CONNACK -> {
                assertEquals(expectedReasonCode, internalClientMessage.payload.connackPayload.get().reasonCode)
            }

            ControlPacketType.DISCONNECT -> assertEquals(expectedReasonCode,
                    internalClientMessage.payload.disconnectPayload.get().reasonCode)

            else -> fail("Missing test code")
        }

    }

    fun sendPINGREQ(client: SimpleClient, l: Location, expectedReasonCode: ReasonCode = ReasonCode.LocationUpdated) {
        client.sendInternalClientMessage(InternalClientMessage(ControlPacketType.PINGREQ, PINGREQPayload(l)))
        val internalClientMessage = client.receiveInternalClientMessage()

        assertEquals(ControlPacketType.PINGRESP, internalClientMessage.controlPacketType)
        assertEquals(expectedReasonCode, internalClientMessage.payload.pingrespPayload.get().reasonCode)
    }

    fun sendSUBSCRIBE(client: SimpleClient, t: Topic, g: Geofence,
                      expectedReasonCode: ReasonCode = ReasonCode.GrantedQoS0) {
        client.sendInternalClientMessage(InternalClientMessage(ControlPacketType.SUBSCRIBE, SUBSCRIBEPayload(t, g)))
        val internalClientMessage = client.receiveInternalClientMessage()

        assertEquals(ControlPacketType.SUBACK, internalClientMessage.controlPacketType)
        assertEquals(expectedReasonCode, internalClientMessage.payload.subackPayload.get().reasonCode)
    }

    fun sendPUBLISH(client: SimpleClient, t: Topic, g: Geofence, c: String) {
        client.sendInternalClientMessage(InternalClientMessage(ControlPacketType.PUBLISH, PUBLISHPayload(t, g, c)))

        // no sense in checking what messages we receive right now, as we receive publish and puback in possible
        // different orders
    }

}