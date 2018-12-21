package de.hasenburg.geofencebroker.scenarios;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.communication.ReasonCode;
import de.hasenburg.geofencebroker.communication.ZMQProcessManager;
import de.hasenburg.geofencebroker.main.Configuration;
import de.hasenburg.geofencebroker.main.SimpleClient;
import de.hasenburg.geofencebroker.main.Utility;
import de.hasenburg.geofencebroker.model.InternalClientMessage;
import de.hasenburg.geofencebroker.model.clients.ClientDirectory;
import de.hasenburg.geofencebroker.model.exceptions.CommunicatorException;
import de.hasenburg.geofencebroker.model.payload.CONNECTPayload;
import de.hasenburg.geofencebroker.model.payload.DISCONNECTPayload;
import de.hasenburg.geofencebroker.model.spatial.Location;
import de.hasenburg.geofencebroker.model.storage.TopicAndGeofenceMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConnectAndDisconnectTest {

	private static final Logger logger = LogManager.getLogger();

	ClientDirectory clientDirectory;
	TopicAndGeofenceMapper topicAndGeofenceMapper;
	ZMQProcessManager processManager;

	@SuppressWarnings("Duplicates")
	@Before
	public void setUp() {
		logger.info("Running test setUp");

		clientDirectory = new ClientDirectory();
		topicAndGeofenceMapper = new TopicAndGeofenceMapper(new Configuration());

		processManager = new ZMQProcessManager();
		processManager.runZMQProcess_Broker("tcp://localhost", 5559, "broker");
		processManager.runZMQProcess_MessageProcessor("message_processor", clientDirectory, topicAndGeofenceMapper);

		assertEquals(0, clientDirectory.getNumberOfClients());
	}

	@After
	public void tearDown() {
		logger.info("Running test tearDown.");
		assertTrue(processManager.tearDown(5000));
	}

	@Test
	public void testOneClient() throws InterruptedException, CommunicatorException {
		SimpleClient client = new SimpleClient(null, "tcp://localhost", 5559, processManager);

		// connect
		client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.CONNECT,
																   new CONNECTPayload(Location.random())));
		assertEquals(ControlPacketType.CONNACK, client.receiveInternalClientMessage().getControlPacketType());

		// check whether client exists
		assertEquals(1, clientDirectory.getNumberOfClients());

		// disconnect
		client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.DISCONNECT,
																   new DISCONNECTPayload(ReasonCode.NormalDisconnection)));

		// check whether disconnected and no more messages received
		Utility.sleepNoLog(1, 0);
		assertEquals(0, clientDirectory.getNumberOfClients());
	}

	@Test
	public void testMultipleClients() throws InterruptedException, CommunicatorException {
		List<SimpleClient> clients = new ArrayList<>();
		int activeConnections = 10;
		Random random = new Random();

		// create clients
		for (int i = 0; i < activeConnections; i++) {
			SimpleClient client = new SimpleClient(null, "tcp://localhost", 5559, processManager);
			clients.add(client);
		}

		// send connects and randomly also disconnect
		for (SimpleClient client : clients) {
			client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.CONNECT,
																	   new CONNECTPayload(Location.random())));
			if (random.nextBoolean()) {
				client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.DISCONNECT,
																		   new DISCONNECTPayload(ReasonCode.NormalDisconnection)));
				activeConnections--;
			}
		}

		// check acknowledgements
		for (SimpleClient client : clients) {
			assertEquals(ControlPacketType.CONNACK, client.receiveInternalClientMessage().getControlPacketType());
		}

		Utility.sleepNoLog(1, 0);
		// check number of active clients
		assertEquals("Wrong number of active clients", activeConnections, clientDirectory.getNumberOfClients());
		logger.info("{} out of {} clients were active, so everything fine", activeConnections, 10);
	}

}
