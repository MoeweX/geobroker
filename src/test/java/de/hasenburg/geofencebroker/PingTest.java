package de.hasenburg.geofencebroker;

import de.hasenburg.geofencebroker.client.BasicClient;
import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.communication.ReasonCode;
import de.hasenburg.geofencebroker.communication.RouterCommunicator;
import de.hasenburg.geofencebroker.model.DealerMessage;
import de.hasenburg.geofencebroker.model.Location;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("ConstantConditions")
public class PingTest {

	private static final Logger logger = LogManager.getLogger();

	// Broker
	RouterCommunicator router;
	BlockingQueue<ZMsg> blockingQueue;
	ConnectionManager connectionManager;
	TaskManager taskManager;

	@SuppressWarnings("Duplicates")
	@Before
	public void setUp() throws Exception {
		logger.info("Running test setUp");
		router = new RouterCommunicator("tcp://localhost", 5559);
		router.init(null);
		blockingQueue = new LinkedBlockingDeque<>();
		router.startReceiving(blockingQueue);

		connectionManager = new ConnectionManager();

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
	public void testPingWhileConnected() throws InterruptedException, CommunicatorException {
		logger.info("RUNNING testPingWhileConnected TEST");

		// connect, ping, and disconnect
		BasicClient client = new BasicClient(null, "tcp://localhost", 5559);
		client.sendCONNECT();

		for (int i = 0; i < 10; i++) {
			client.sendPINGREQ();
			Thread.sleep(100);
		}

		client.sendDISCONNECT();

		// check dealer messages
		for (int i = 0; i < 11; i++) {
			assertEquals("Dealer queue contains wrong number of elements.", 11 - i, client.blockingQueue.size());
			Optional<DealerMessage> dealerMessage = DealerMessage.buildDealerMessage(client.blockingQueue.poll(1, TimeUnit.SECONDS));
			assertTrue("DealerMessage is missing", dealerMessage.isPresent());
			if (i == 0) {
				dealerMessage.ifPresent(message -> assertEquals(ControlPacketType.CONNACK, message.getControlPacketType()));
			} else {
				dealerMessage.ifPresent(message -> {
					assertEquals(ControlPacketType.PINGRESP, message.getControlPacketType());
					assertEquals(ReasonCode.LocationUpdated, message.getPayload().getPINGRESPPayload().get().getReasonCode());
				});
			}
		}

		client.tearDown();
		logger.info("FINISHED TEST");
	}

	@SuppressWarnings("ConstantConditions")
	@Test
	public void testPingWhileNotConnected() throws InterruptedException, CommunicatorException {
		logger.info("RUNNING testPingWhileConnected TEST");

		// connect, ping, and disconnect
		BasicClient client = new BasicClient(null, "tcp://localhost", 5559);

		client.sendPINGREQ();

		Optional<DealerMessage> dealerMessage = DealerMessage.buildDealerMessage(client.blockingQueue.poll(1, TimeUnit.SECONDS));
		assertTrue("DealerMessage is missing", dealerMessage.isPresent());
		dealerMessage.ifPresent(message -> {
			assertEquals(ControlPacketType.PINGRESP, message.getControlPacketType());
			assertEquals(ReasonCode.NotConnected, message.getPayload().getPINGRESPPayload().get().getReasonCode());
		});

		client.tearDown();
		logger.info("FINISHED TEST");
	}

}
