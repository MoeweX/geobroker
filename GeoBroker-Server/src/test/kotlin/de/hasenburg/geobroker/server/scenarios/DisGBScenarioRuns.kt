package de.hasenburg.geobroker.server.scenarios

import de.hasenburg.geobroker.client.main.SimpleClient
import de.hasenburg.geobroker.commons.communication.ZMQProcessManager
import de.hasenburg.geobroker.commons.model.message.Payload.*
import de.hasenburg.geobroker.commons.model.message.ReasonCode
import de.hasenburg.geobroker.commons.model.message.Topic
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.commons.setLogLevel
import de.hasenburg.geobroker.commons.sleepNoLog
import de.hasenburg.geobroker.server.main.Configuration
import de.hasenburg.geobroker.server.main.server.DisGBPublisherMatchingServerLogic
import de.hasenburg.geobroker.server.main.server.DisGBSubscriberMatchingServerLogic
import de.hasenburg.geobroker.server.main.server.IServerLogic
import de.hasenburg.geobroker.server.storage.client.ClientDirectory
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.LogManager
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test

private var logger = LogManager.getLogger()

class DisGBScenarioRuns {

    // areas must be compatible to disgb_scenario.json
    private val parisArea = Geofence.circle(Location(48.86, 2.35), 3.0)
    private val berlinArea = Geofence.circle(Location(52.52, 13.4), 3.0)

    private val topics = arrayOf(Topic("data/1"), Topic("data/2"), Topic("data/3"))

    // client locations for experiments
    private val cl1 = Location(45.87, 2.3)
    private val cl2 = Location(48.0, 2.0)
    private val cl3 = Location(52.0, 13.0)

    // geofences for experiments
    private val sg1 = Geofence.rectangle(Location(46.5, 1.0), Location(54.0, 14.0))
    private val sg2 = Geofence.circle(Location(48.86, 2.35), 1.5)
    private val sg3 = Geofence.circle(Location(52.52, 13.4), 1.5)

    private val mg1 = Geofence.rectangle(Location(46.4, 0.9), Location(54.1, 14.1))
    private val mg2 = Geofence.circle(Location(48.86, 2.35), 1.6)
    private val mg3 = Geofence.circle(Location(52.52, 13.4), 1.6)

    // client fields
    private lateinit var clientProcessManager: ZMQProcessManager
    private lateinit var clients: List<SimpleClient>

    @Before
    fun setUp() {
        setLogLevel(logger, Level.INFO)
        clientProcessManager = ZMQProcessManager()
        clients = setupLocalhostClients(listOf(5558, 5558, 5559))

        logger.info("Client setup completed")
    }

    @After
    fun tearDown() {
        for (client in clients) {
            client.tearDownClient()
        }
        clientProcessManager.tearDown(1000)
        logger.info("Client teardown completed")
    }

    @Test
    fun validateVariables() {
        // broker areas
        assertFalse(parisArea.intersects(berlinArea))
        assertTrue(parisArea.contains(cl1))
        assertTrue(parisArea.contains(cl2))
        assertTrue(berlinArea.contains(cl3))

        // check intersection of geofences with broker areas
        assertTrue(parisArea.intersects(mg1))
        assertTrue(parisArea.intersects(mg2))
        assertFalse(parisArea.intersects(mg3))
        assertTrue(berlinArea.intersects(mg1))
        assertFalse(berlinArea.intersects(mg2))
        assertTrue(berlinArea.intersects(mg3))

        assertTrue(parisArea.intersects(sg1))
        assertTrue(parisArea.intersects(sg2))
        assertFalse(parisArea.intersects(sg3))
        assertTrue(berlinArea.intersects(sg1))
        assertFalse(berlinArea.intersects(sg2))
        assertTrue(berlinArea.intersects(sg3))

        // subscription geofences and client locations
        assertFalse(sg1.contains(cl1))
        assertTrue(sg1.contains(cl2))
        assertTrue(sg1.contains(cl3))

        assertFalse(sg2.contains(cl1))
        assertTrue(sg2.contains(cl2))
        assertFalse(sg2.contains(cl3))

        assertFalse(sg3.contains(cl1))
        assertFalse(sg3.contains(cl2))
        assertTrue(sg3.contains(cl3))

        // message geofences and client locations
        assertFalse(mg1.contains(cl1))
        assertTrue(mg1.contains(cl2))
        assertTrue(mg1.contains(cl3))

        assertFalse(mg2.contains(cl1))
        assertTrue(mg2.contains(cl2))
        assertFalse(mg2.contains(cl3))

        assertFalse(mg3.contains(cl1))
        assertFalse(mg3.contains(cl2))
        assertTrue(mg3.contains(cl3))
    }

    @Test
    fun subscriberMatchingScenario() {
        val paris = DisGBSubscriberMatchingServerLogic()
        val berlin = DisGBSubscriberMatchingServerLogic()
        startDisGBServers(paris, "disgb_SMscenario-paris.toml", berlin, "disgb_SMscenario-berlin.toml")

        logger.info("Starting subscriber matching run\n\n\n")

        /* ***************************************************************
         * Connect to respective broker
         ****************************************************************/

        doConnect()
        // validate connects internally
        assertEquals(2, paris.clientDirectory.numberOfClients)
        assertEquals(1, berlin.clientDirectory.numberOfClients)

        /* ***************************************************************
         * Update location to what is needed for the experiment
         ****************************************************************/

        doPing()
        // validate whether brokers got locations
        assertEquals(cl1, paris.clientDirectory.getClientLocation(getClientIdentifier(0)))
        assertEquals(cl2, paris.clientDirectory.getClientLocation(getClientIdentifier(1)))
        assertEquals(cl3, berlin.clientDirectory.getClientLocation(getClientIdentifier(2)))

        /* ***************************************************************
         * Create subscriptions
         ****************************************************************/

        doSubscribe()
        // validate whether brokers got subscriptions (we check mostly the count, but one do we actually check)
        assertNotNull(paris.clientDirectory.getSubscription(getClientIdentifier(0), topics[0]))
        assertNull(berlin.clientDirectory.getSubscription(getClientIdentifier(0), topics[0]))
        assertEquals(3, paris.clientDirectory.getCurrentClientSubscriptions(getClientIdentifier(0)))
        assertEquals(3, paris.clientDirectory.getCurrentClientSubscriptions(getClientIdentifier(1)))
        assertEquals(0, paris.clientDirectory.getCurrentClientSubscriptions(getClientIdentifier(2)))
        assertEquals(0, berlin.clientDirectory.getCurrentClientSubscriptions(getClientIdentifier(0)))
        assertEquals(0, berlin.clientDirectory.getCurrentClientSubscriptions(getClientIdentifier(1)))
        assertEquals(3, berlin.clientDirectory.getCurrentClientSubscriptions(getClientIdentifier(2)))


        /* ***************************************************************
         * Publish message
         ****************************************************************/

        doPublish()

        /* ***************************************************************
         * Update subscription data/1 to only affect a single broker area
         ****************************************************************/

        doOverwritingSubscribe(paris.clientDirectory)

        /* ***************************************************************
         * Unsubscribe again
         ****************************************************************/

        doUnsubscribe()

        /* ***************************************************************
         * Disconnect
         ****************************************************************/

        doDisconnect(paris.clientDirectory, berlin.clientDirectory)

        /* ***************************************************************
         * Final checks
         ****************************************************************/

        assertEquals(0, paris.notAcknowledgedMessages())
        assertEquals(0, berlin.notAcknowledgedMessages())

        logger.info("Finished subscriber matching run\n\n\n")
        stopDisGBServers(paris, berlin)
    }

    private fun doDisconnect(parisCD: ClientDirectory, berlinCD: ClientDirectory) {
        for (i in 0..2) {
            sendDISCONNECT(clients[i])
        }

        sleepNoLog(100, 0) // wait until disconnected
        assertEquals(0, parisCD.numberOfClients)
        assertEquals(0, berlinCD.numberOfClients)
    }

    @Test
    fun publisherMatchingScenario() {
        val paris = DisGBPublisherMatchingServerLogic()
        val berlin = DisGBPublisherMatchingServerLogic()
        startDisGBServers(paris, "disgb_PMscenario-paris.toml", berlin, "disgb_PMscenario-berlin.toml")

        logger.info("Starting publisher matching run\n\n\n")

        /* ***************************************************************
         * Connect to respective broker
         ****************************************************************/

        doConnect()
        // validate connects internally
        assertEquals(2, paris.clientDirectory.numberOfClients)
        assertEquals(1, berlin.clientDirectory.numberOfClients)

        /* ***************************************************************
         * Update location to what is needed for the experiment
         ****************************************************************/

        doPing()
        // validate whether brokers got locations
        assertEquals(cl1, paris.clientDirectory.getClientLocation(getClientIdentifier(0)))
        assertEquals(cl2, paris.clientDirectory.getClientLocation(getClientIdentifier(1)))
        assertEquals(cl3, berlin.clientDirectory.getClientLocation(getClientIdentifier(2)))

        /* ***************************************************************
         * Create subscriptions
         ****************************************************************/

        doSubscribe()
        // validate whether brokers got subscriptions (we check mostly the count, but two do we actually check)
        // sub1 should be available at both brokers for client 1
        assertNotNull(paris.clientDirectory.getSubscription(getClientIdentifier(0), topics[0]))
        assertNotNull(berlin.clientDirectory.getSubscription(getClientIdentifier(0), topics[0]))
        // sub2 should be available only in paris for client 1
        assertNotNull(paris.clientDirectory.getSubscription(getClientIdentifier(0), topics[1]))
        assertNull(berlin.clientDirectory.getSubscription(getClientIdentifier(0), topics[1]))

        // we need to wait as subscription forwarding takes some time
        sleepNoLog(100, 0)

        assertEquals(3, paris.clientDirectory.getCurrentClientSubscriptions(getClientIdentifier(0)))
        assertEquals(3, paris.clientDirectory.getCurrentClientSubscriptions(getClientIdentifier(1)))
        assertEquals(2, paris.clientDirectory.getCurrentClientSubscriptions(getClientIdentifier(2)))
        assertEquals(2, berlin.clientDirectory.getCurrentClientSubscriptions(getClientIdentifier(0)))
        assertEquals(2, berlin.clientDirectory.getCurrentClientSubscriptions(getClientIdentifier(1)))
        assertEquals(3, berlin.clientDirectory.getCurrentClientSubscriptions(getClientIdentifier(2)))

        // the subscriptions should have lead to additional clients being created
        assertEquals(3, paris.clientDirectory.numberOfClients)
        assertEquals(3, berlin.clientDirectory.numberOfClients)

        /* ***************************************************************
         * Publish message
         ****************************************************************/

        doPublish()

        /* ***************************************************************
         * Update subscription data/1 to only affect a single broker area
         ****************************************************************/

        doOverwritingSubscribe(paris.clientDirectory)
        // validate that cl3 and data/1 subscription are back at paris
        assertNotNull(paris.clientDirectory.getSubscription(getClientIdentifier(2), topics[0]))

        /* ***************************************************************
         * Unsubscribe again
         ****************************************************************/

        doUnsubscribe()

        /* ***************************************************************
         * Disconnect
         ****************************************************************/

        doDisconnect(paris.clientDirectory, berlin.clientDirectory)

        /* ***************************************************************
         * Final checks
         ****************************************************************/

        assertEquals(0, paris.notAcknowledgedMessages())
        assertEquals(0, berlin.notAcknowledgedMessages())

        logger.info("Finished publisher matching run\n\n\n")
        stopDisGBServers(paris, berlin)
    }

    private fun doUnsubscribe() {
        for (i in 0..2) {
            sendUNSUBSCRIBE(clients[i], topics[0])
            sendUNSUBSCRIBE(clients[i], topics[1])
            sendUNSUBSCRIBE(clients[i], topics[2])
        }

        // publish MG_1 again, but no one should get it

        for (i in 0..2) {
            logger.info("Publishing with message geofence $mg1 to topic ${topics[i]}")
            sendPUBLISH(clients[1], topics[i], mg1, generateContent(1, topics[i]))
        }

        validateReceivedMessagesForClient(clients[0], 0, 0)
        validateReceivedMessagesForClient(clients[1], 3, 0)
        validateReceivedMessagesForClient(clients[2], 0, 0)
    }

    private fun doOverwritingSubscribe(clientDirectory: ClientDirectory) {
        // update data/1 subscription to use sg3
        // --> when cl2 publishes to data/1 -> no one receives anything
        // --> when cl3 publishes to data/1 -> cl2 and cl3 receive a single message
        for (i in 0..2) {
            sendSUBSCRIBE(clients[i], topics[0], sg3)
        }
        // we need to wait as subscription forwarding takes some time in case of publisher matching
        sleepNoLog(100, 0)
        sendPUBLISH(clients[1], topics[0], mg1, generateContent(1, topics[0]))
        validateReceivedMessagesForClient(clients[0], 0, 0)
        validateReceivedMessagesForClient(clients[1], 1, 0)
        validateReceivedMessagesForClient(clients[2], 0, 0)
        sendPUBLISH(clients[2], topics[0], mg1, generateContent(2, topics[0]))
        validateReceivedMessagesForClient(clients[0], 0, 0)
        validateReceivedMessagesForClient(clients[1], 0, 1)
        validateReceivedMessagesForClient(clients[2], 1, 1)
        // paris should not have an active subscription for cl3 and data/1 anymore
        assertNull(clientDirectory.getSubscription(getClientIdentifier(2), topics[0]))

        // update data/1 subscription back to use sg1
        // --> when cl2 publishes to data/1 -> cl2 and cl3 receive a single message
        // --> when cl3 publishes to data/1 -> cl2 and cl3 receive a single message
        for (i in 0..2) {
            sendSUBSCRIBE(clients[i], topics[0], sg1)
        }
        // we need to wait as subscription forwarding takes some time in case of publisher matching
        sleepNoLog(100, 0)
        sendPUBLISH(clients[1], topics[0], mg1, generateContent(1, topics[0]))
        validateReceivedMessagesForClient(clients[0], 0, 0)
        validateReceivedMessagesForClient(clients[1], 1, 1)
        validateReceivedMessagesForClient(clients[2], 0, 1)
        sendPUBLISH(clients[2], topics[0], mg1, generateContent(2, topics[0]))
        validateReceivedMessagesForClient(clients[0], 0, 0)
        validateReceivedMessagesForClient(clients[1], 0, 1)
        validateReceivedMessagesForClient(clients[2], 1, 1)
    }

    /*****************************************************************
     * Run Operation Helper
     ****************************************************************/

    private fun doConnect() {
        // forbidden connect
        sendCONNECT(clients[0], Location.randomInGeofence(berlinArea), true)

        // correct connect
        sendCONNECT(clients[0], Location.randomInGeofence(parisArea))
        sendCONNECT(clients[1], Location.randomInGeofence(parisArea))
        sendCONNECT(clients[2], Location.randomInGeofence(berlinArea))
    }

    private fun doPing() {
        sendPINGREQ(clients[0], cl1)
        sendPINGREQ(clients[1], cl2)
        sendPINGREQ(clients[2], cl3)
    }

    private fun doSubscribe() {
        // all clients create the same subscriptions
        for (i in 0..2) {
            sendSUBSCRIBE(clients[i], topics[0], sg1)
            sendSUBSCRIBE(clients[i], topics[1], sg2)
            sendSUBSCRIBE(clients[i], topics[2], sg3)
        }
    }

    private fun doPublish() {
        // we need to publish to every topic, as the subscriptions are generated per topic and we want to match against
        // every subscription (that's why i in 0..2)

        // --------
        // MG1
        // --------

        for (i in 0..2) {
            logger.info("Publishing with message geofence $mg1 to topic ${topics[i]}")
            sendPUBLISH(clients[1], topics[i], mg1, generateContent(1, topics[i]))
        }

        // validate for each client the received messages
        validateReceivedMessagesForClient(clients[0], 0, 0)
        validateReceivedMessagesForClient(clients[1], 3, 2)
        validateReceivedMessagesForClient(clients[2], 0, 2)

        // --------
        // MG2
        // --------

        for (i in 0..2) {
            logger.info("Publishing with message geofence $mg2 to topic ${topics[i]}")
            sendPUBLISH(clients[1], topics[i], mg2, generateContent(1, topics[i]))
        }

        // validate for each client the received messages
        validateReceivedMessagesForClient(clients[0], 0, 0)
        validateReceivedMessagesForClient(clients[1], 3, 2)
        validateReceivedMessagesForClient(clients[2], 0, 0)

        // --------
        // MG3
        // --------

        for (i in 0..2) {
            logger.info("Publishing with message geofence $mg3 to topic ${topics[i]}")
            sendPUBLISH(clients[1], topics[i], mg3, generateContent(1, topics[i]))
        }

        // validate for each client the received messages
        validateReceivedMessagesForClient(clients[0], 0, 0)
        validateReceivedMessagesForClient(clients[1], 3, 0)
        validateReceivedMessagesForClient(clients[2], 0, 2)
    }

    /*****************************************************************
     * Miscellaneous Helper
     ****************************************************************/

    private fun getClientIdentifier(index: Int): String {
        return "Client-${index + 1}"
    }

    private fun generateContent(index: Int, t: Topic): String {
        return "This message was published by client ${getClientIdentifier(index)} to topic $t"
    }

    private fun startDisGBServers(paris: IServerLogic, parisConf: String, berlin: IServerLogic, berlinConf: String) {
        val configurations = listOf(parisConf, berlinConf)

        for ((i, lifecycle) in listOf(paris, berlin).withIndex()) {
            val c = Configuration.readInternalConfiguration(configurations[i])
            lifecycle.loadConfiguration(c)
            lifecycle.initializeFields()
            lifecycle.startServer()
            logger.info("Started server ${c.brokerId}")
        }
    }

    private fun stopDisGBServers(paris: IServerLogic, berlin: IServerLogic) {
        paris.cleanUp()
        berlin.cleanUp()
    }

    private fun setupLocalhostClients(ports: List<Int>): List<SimpleClient> {
        val clients = mutableListOf<SimpleClient>()

        for (i in ports.indices) {
            val client = SimpleClient("localhost", ports[i], clientProcessManager, getClientIdentifier(i))
            clients.add(client)
        }

        return clients
    }

    private fun validateReceivedMessagesForClient(client: SimpleClient, pubacks: Int, publishs: Int) {
        var remainingPublishs = publishs
        var remainingPubacks = pubacks
        for (i in 1..remainingPubacks + remainingPublishs) {
            val payload = client.receiveWithTimeout(1000)!!

            if (payload is PUBLISHPayload) {
                remainingPublishs--
                logger.debug("Received a PUBLISH message from server: {}", payload)
            } else if (payload is PUBACKPayload) {
                remainingPubacks--
                logger.debug("Received a PUBACK message from server: {}", payload)
            }
        }
        assertEquals(0, remainingPubacks)
        assertEquals(0, remainingPublishs)
        assertNull(client.receiveWithTimeout(0)) // no more messages
    }

    /*****************************************************************
     * Send helper
     ****************************************************************/

    private fun sendCONNECT(client: SimpleClient, l: Location, disconnect: Boolean = false) {

        client.send(CONNECTPayload(l))
        val responsePayload = client.receiveWithTimeout(1000)

        if (!disconnect) {
            assertEquals(CONNACKPayload(ReasonCode.Success), responsePayload)
        } else {
            assertTrue(responsePayload is DISCONNECTPayload)
        }
    }

    private fun sendDISCONNECT(client: SimpleClient) {
        client.send(DISCONNECTPayload(ReasonCode.NormalDisconnection))
        // there is no reply to disconnect
    }

    private fun sendPINGREQ(client: SimpleClient, l: Location,
                            expectedReasonCode: ReasonCode = ReasonCode.LocationUpdated) {
        client.send(PINGREQPayload(l))
        val payload = client.receiveWithTimeout(1000)

        if (payload is PINGRESPPayload) {
            assertEquals(expectedReasonCode, payload.reasonCode)
        } else {
            fail("Wrong payload, received $payload")
        }
    }

    private fun sendSUBSCRIBE(client: SimpleClient, t: Topic, g: Geofence,
                              expectedReasonCode: ReasonCode = ReasonCode.GrantedQoS0) {
        client.send(SUBSCRIBEPayload(t, g))
        val payload = client.receiveWithTimeout(1000)

        if (payload is SUBACKPayload) {
            assertEquals(expectedReasonCode, payload.reasonCode)
        } else {
            fail("Wrong payload, received $payload")
        }
    }

    private fun sendUNSUBSCRIBE(client: SimpleClient, t: Topic, expectedReasonCode: ReasonCode = ReasonCode.Success) {
        client.send(UNSUBSCRIBEPayload(t))
        val payload = client.receiveWithTimeout(1000)

        if (payload is UNSUBACKPayload) {
            assertEquals(expectedReasonCode, payload.reasonCode)
        } else {
            fail("Wrong payload, received $payload")
        }
    }

    private fun sendPUBLISH(client: SimpleClient, t: Topic, g: Geofence, c: String) {
        client.send(PUBLISHPayload(t, g, c))
        // no sense in checking what messages we receive right now, as we receive publish and puback in possible
        // different orders -> has to be checked later
    }

}