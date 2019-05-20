package de.hasenburg.geobroker.server.scenarios.disgb

import de.hasenburg.geobroker.client.communication.InternalClientMessage
import de.hasenburg.geobroker.client.main.SimpleClient
import de.hasenburg.geobroker.commons.communication.ZMQProcessManager
import de.hasenburg.geobroker.commons.model.message.ControlPacketType
import de.hasenburg.geobroker.commons.model.message.ReasonCode
import de.hasenburg.geobroker.commons.model.message.payloads.CONNECTPayload
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.server.distribution.BrokerArea
import de.hasenburg.geobroker.server.main.Configuration
import de.hasenburg.geobroker.server.main.server.DisGBSubscriberMatchingServerLogic
import de.hasenburg.geobroker.server.main.server.IServerLogic
import org.apache.logging.log4j.LogManager
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test

private var logger = LogManager.getLogger()

class SubscriberMatchingRun {

    private val configurationFile = arrayOf("disgb_scenario-berlin.toml", "disgb_scenario-paris.toml")
    // areas must be compatible to disgb_scenario.json
    private val berlinArea = Geofence.circle(Location(52.52, 13.4), 3.0)
    private val parisArea = Geofence.circle(Location(48.86, 2.35), 3.0)


    // other fields
    private lateinit var berlin: IServerLogic
    private lateinit var paris: IServerLogic

    private lateinit var clientProcessManager: ZMQProcessManager
    private lateinit var clients: List<SimpleClient>

    @Before
    fun setUp() {
        berlin = DisGBSubscriberMatchingServerLogic()
        paris = DisGBSubscriberMatchingServerLogic()

        for ((i, lifecycle) in listOf(berlin, paris).withIndex()) {
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
        berlin.cleanUp()
        paris.cleanUp()
        logger.info("Teardown completed")
    }

    @Test
    fun setUpTearDown() {
        logger.info("Servers seem to be running")
    }

    @Test
    fun scenario() {
        logger.info("Starting scenario\n\n\n")

        /* ***************************************************************
         * Connect to respective broker
         ****************************************************************/

        // forbidden connect

        // correct connect
        sendConnect(clients[0], Location.randomInGeofence(berlinArea))
        sendConnect(clients[1], Location.randomInGeofence(berlinArea))
        sendConnect(clients[2], Location.randomInGeofence(parisArea))

        // validate connects internally

        logger.info("Finished scenario\n\n\n")
    }

    fun setupLocalhostClients(ports: List<Int>) : List<SimpleClient> {
        val clients = mutableListOf<SimpleClient>()

        for (i in 1..ports.size) {
            val client = SimpleClient("Client-$i", "localhost", ports[i-1], clientProcessManager)
            clients.add(client)
        }

        return clients
    }

    fun sendConnect(client: SimpleClient, l: Location, expectedReasonCode: ReasonCode = ReasonCode.Success) {
        client.sendInternalClientMessage(InternalClientMessage(ControlPacketType.CONNECT, CONNECTPayload(l)))
        val internalClientMessage = client.receiveInternalClientMessage()

        // validate response
        assertEquals(ControlPacketType.CONNACK, internalClientMessage.controlPacketType)
        assertEquals(expectedReasonCode, internalClientMessage.payload.connackPayload.get().reasonCode)
    }

}