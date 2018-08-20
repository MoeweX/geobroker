package de.hasenburg.geofencebroker;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.communication.ZMQProcessManager;
import de.hasenburg.geofencebroker.model.InternalClientMessage;
import de.hasenburg.geofencebroker.model.connections.ConnectionManager;
import de.hasenburg.geofencebroker.model.exceptions.CommunicatorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConnectAndDisconnectTest {

	private static final Logger logger = LogManager.getLogger();

	ConnectionManager connectionManager;
	ZMQProcessManager processManager;

	@SuppressWarnings("Duplicates")
	@Before
	public void setUp() {
		logger.info("Running test setUp");

		connectionManager = new ConnectionManager();

		processManager = new ZMQProcessManager();
		processManager.runZMQProcess_Broker("tcp://localhost", 5559, "broker");
		processManager.runZMQProcess_MessageProcessor("message_processor", connectionManager);
	}

	@After
	public void tearDown() {
		logger.info("Running test tearDown.");
		assertTrue(processManager.tearDown(5000));
	}

	@Test
	public void testOneClient() throws InterruptedException, CommunicatorException {
		logger.info("RUNNING testOneClient TEST");
		TestClient client = new TestClient(null, "tcp://localhost", 5559);
		client.sendCONNECT();
		client.sendDISCONNECT();

		// check dealer messages
		Optional<InternalClientMessage> dealerMessage = InternalClientMessage
				.buildMessage(client.blockingQueue.poll(1, TimeUnit.SECONDS));
		assertEquals("Dealer queue should not contain any more elements.", 0, client.blockingQueue.size());
		assertTrue("InternalClientMessage is missing", dealerMessage.isPresent());
		dealerMessage.ifPresent(message -> assertEquals(ControlPacketType.CONNACK, message.getControlPacketType()));

		// check if connection is inactive
		Thread.sleep(100);
		assertEquals("No connection should exist", 0, connectionManager.getActiveConnections().size());

		client.tearDown();
		logger.info("FINISHED TEST");
	}

	@Test
	public void testMultipleClients() throws InterruptedException, CommunicatorException {
		logger.info("RUNNING testMultipleClients TEST");

		List<TestClient> clients = new ArrayList<>();
		int activeConnections = 10;
		Random random = new Random();

		// create clients
		for (int i = 0; i < activeConnections; i++) {
			TestClient client = new TestClient(null, "tcp://localhost", 5559);
			clients.add(client);
		}

		// wait for clients to setup
		Thread.sleep(3000);

		// send connects and randomly also disconnect
		for (TestClient client : clients) {
			client.sendCONNECT();
			if (random.nextBoolean()) {
				client.sendDISCONNECT();
				activeConnections--;
			}
		}

		// check acknowledgements
		for (TestClient client : clients) {
			Optional<InternalClientMessage> dealerMessage = InternalClientMessage
					.buildMessage(client.blockingQueue.poll(3, TimeUnit.SECONDS));
			assertEquals("Queue should not contain any more elements.", 0, client.blockingQueue.size());
			assertTrue("InternalClientMessage is missing", dealerMessage.isPresent());

			dealerMessage.ifPresent(message -> assertEquals(ControlPacketType.CONNACK, message.getControlPacketType()));
		}

		Thread.sleep(100);
		// check number of active connections
		assertEquals("Wrong number of active connections", activeConnections, connectionManager.getActiveConnections().size());

		// tear down clients
		clients.forEach(c -> c.tearDown());

		// wait for receiving to stop
		logger.info("FINISHED TEST");
	}

}
