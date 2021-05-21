package de.hasenburg.geobroker.server.scenarios

import de.hasenburg.geobroker.client.main.SimpleClient
import de.hasenburg.geobroker.commons.*
import de.hasenburg.geobroker.commons.communication.ZMQProcessManager
import de.hasenburg.geobroker.commons.model.disgb.BrokerArea
import de.hasenburg.geobroker.commons.model.message.ReasonCode
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.server.main.server.DisGBSubscriberMatchingServerLogic
import org.apache.logging.log4j.LogManager
import org.junit.After
import org.junit.Before
import org.junit.Test

import java.util.ArrayList
import java.util.Random

import de.hasenburg.geobroker.commons.model.message.Payload.*
import de.hasenburg.geobroker.server.main.readInternalConfiguration
import de.hasenburg.geobroker.server.main.server.SingleGeoBrokerServerLogic
import io.prometheus.client.CollectorRegistry
import org.junit.Assert.*
import org.junit.Ignore

class ConnectAndDisconnectTest {

    private val logger = LogManager.getLogger()
    private lateinit var serverLogic: SingleGeoBrokerServerLogic

    @Before
    fun setUp() {
        logger.info("Running test setUp")
        CollectorRegistry.defaultRegistry.clear();

        serverLogic = SingleGeoBrokerServerLogic()
        serverLogic.loadConfiguration(readInternalConfiguration("connect_and_disconnect.toml"))
        serverLogic.initializeFields()
        serverLogic.startServer()

        assertEquals(0, serverLogic.clientDirectory.numberOfClients.toLong())
    }

    @After
    fun tearDown() {
        logger.info("Running test tearDown.")
        serverLogic.cleanUp()
    }

    @Test
    fun testOneClient() {
        val client = SimpleClient("localhost", 5559)

        // connect
        client.send(CONNECTPayload(Location.random()))
        assertTrue(client.receiveWithTimeout(100) is CONNACKPayload)

        // check whether client exists
        assertEquals(1, serverLogic.clientDirectory.numberOfClients.toLong())

        // disconnect
        client.send(DISCONNECTPayload(ReasonCode.NormalDisconnection))

        // check whether disconnected and no more messages received
        sleepNoLog(5, 0)
        assertEquals(0, serverLogic.clientDirectory.numberOfClients.toLong())

        client.tearDownClient()
    }

    @Test
    fun testMultipleClients() {
        val clients = ArrayList<SimpleClient>()
        var activeConnections = 10
        val random = Random()

        // create clients
        for (i in 0 until activeConnections) {
            val client = SimpleClient("localhost", 5559)
            clients.add(client)
        }

        // send connects and randomly also disconnect
        for (client in clients) {
            client.send(CONNECTPayload(Location.random()))
            if (random.nextBoolean()) {
                client.send(DISCONNECTPayload(ReasonCode.NormalDisconnection))
                activeConnections--
            }
        }

        // check acknowledgements
        for (client in clients) {
            assertTrue(client.receiveWithTimeout(100) is CONNACKPayload)
        }

        sleepNoLog(1, 0)
        // check number of active clients
        assertEquals("Wrong number of active clients",
                activeConnections.toLong(),
                serverLogic.clientDirectory.numberOfClients.toLong())
        logger.info("{} out of {} clients were active, so everything fine", activeConnections, 10)

        // tear down clients
        for (client in clients) {
            client.tearDownClient()
        }

    }

    @Ignore
    @Test
    fun testNotResponsibleClient() {
        TODO("REQUIRES DisGBSubscriberMatchingServerLogic")
//        serverLogic.brokerAreaManager.updateOwnBrokerArea(BrokerArea(serverLogic.brokerAreaManager
//            .ownBrokerInfo,
//                Geofence.circle(Location(0.0, 0.0), 10.0)))

        val client = SimpleClient("localhost", 5559)

        // connect
        client.send(CONNECTPayload(Location(30.0, 30.0)))
        val payload = client.receiveWithTimeout(100)
        logger.info("Client received response {}", payload)
        if (payload is DISCONNECTPayload) {
            assertEquals(ReasonCode.WrongBroker, payload.reasonCode)
        } else {
            fail("Wrong payload, received $payload")
        }

        // check whether client exists
        assertEquals(0, serverLogic.clientDirectory.numberOfClients.toLong())

        // check acknowledgements
        client.tearDownClient()
    }

}
