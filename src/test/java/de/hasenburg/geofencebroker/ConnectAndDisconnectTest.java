package de.hasenburg.geofencebroker;

import de.hasenburg.geofencebroker.client.BasicClient;
import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.communication.RouterCommunicator;
import de.hasenburg.geofencebroker.model.DealerMessage;
import de.hasenburg.geofencebroker.model.connections.ConnectionManager;
import de.hasenburg.geofencebroker.model.exceptions.CommunicatorException;
import de.hasenburg.geofencebroker.tasks.TaskManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zeromq.ZMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConnectAndDisconnectTest {

	private static final Logger logger = LogManager.getLogger();

	// Broker
	RouterCommunicator router;
	BlockingQueue<ZMsg> blockingQueue;
	ConnectionManager connectionManager;
	TaskManager taskManager;

	@Before
	public void setUp() throws Exception {
		logger.info("Running test setUp");
		router = new RouterCommunicator("tcp://localhost", 5559);
		router.init(null);
		blockingQueue = new LinkedBlockingDeque<>();
		router.startReceiving(blockingQueue);

		connectionManager = new ConnectionManager(router);

		taskManager = new TaskManager();
		taskManager.runMessageProcessorTask(blockingQueue, router, connectionManager);
	}

	@After
	public void tearDown() throws Exception {
		logger.info("Running test tearDown.");
		router.tearDown();
		taskManager.tearDown();
	}

	@Test
	public void testOneClient() throws InterruptedException, CommunicatorException {
		logger.info("RUNNING testOneClient TEST");
		BasicClient client = new BasicClient(null, "tcp://localhost", 5559);
		client.sendCONNECT();
		client.sendDISCONNECT();

		// check dealer messages
		Optional<DealerMessage> dealerMessage = DealerMessage.buildDealerMessage(client.blockingQueue.poll(1, TimeUnit.SECONDS));
		assertEquals("Dealer queue should not contain any more elements.", 0, client.blockingQueue.size());
		assertTrue("DealerMessage is missing", dealerMessage.isPresent());
		dealerMessage.ifPresent(message -> assertEquals(ControlPacketType.CONNACK, message.getControlPacketType()));

		// check if connection is inactive
		Thread.sleep(100);
		assertEquals("Connection of client should be inactive", client.getIdentity(), connectionManager.getInactiveConnections().get(0).getClientIdentifier());

		client.tearDown();
		logger.info("FINISHED TEST");
	}

	@Test
	public void testMultipleClients() throws InterruptedException, CommunicatorException {
		logger.info("RUNNING testMultipleClients TEST");

		List<BasicClient> clients = new ArrayList<>();
		int activeConnections = 200;
		Random random = new Random();

		// create clients
		for (int i = 0; i < activeConnections; i++) {
			BasicClient client = new BasicClient(null, "tcp://localhost", 5559);
			clients.add(client);
		}

		// wait for clients to setup
		Thread.sleep(3000);

		// send connects and randomly also disconnect
		for (BasicClient client : clients) {
			client.sendCONNECT();
			if (random.nextBoolean()) {
				client.sendDISCONNECT();
				activeConnections--;
			}
		}

		// check acknowledgements
		for (BasicClient client : clients) {
			Optional<DealerMessage> dealerMessage = DealerMessage.buildDealerMessage(client.blockingQueue.poll(3, TimeUnit.SECONDS));
			assertEquals("Queue should not contain any more elements.", 0, client.blockingQueue.size());
			assertTrue("DealerMessage is missing", dealerMessage.isPresent());

			dealerMessage.ifPresent(message -> assertEquals(ControlPacketType.CONNACK, message.getControlPacketType()));
		}

		Thread.sleep(100);
		// check number of active and inactive connections
		assertEquals("Wrong number of active connections", activeConnections, connectionManager.getActiveConnections().size());
		assertEquals("Wrong number of inactive connections", clients.size() - activeConnections, connectionManager.getInactiveConnections().size());

		// tear down clients
		clients.forEach(c -> c.tearDown());

		// wait for receiving to stop
		logger.info("FINISHED TEST");
	}

}
