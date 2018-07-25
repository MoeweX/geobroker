package de.hasenburg.geofencebroker;

import de.hasenburg.geofencebroker.client.BasicClient;
import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.communication.RouterCommunicator;
import de.hasenburg.geofencebroker.model.DealerMessage;
import de.hasenburg.geofencebroker.model.Location;
import de.hasenburg.geofencebroker.model.Topic;
import de.hasenburg.geofencebroker.model.connections.ConnectionManager;
import de.hasenburg.geofencebroker.model.exceptions.CommunicatorException;
import de.hasenburg.geofencebroker.model.geofence.Geofence;
import de.hasenburg.geofencebroker.model.payload.PUBLISHPayload;
import de.hasenburg.geofencebroker.tasks.TaskManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zeromq.ZMsg;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PublishSubscribeTest {

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

	@SuppressWarnings("ConstantConditions")
	@Test
	public void testSubscribeInGeofence() throws InterruptedException, CommunicatorException {
		logger.info("RUNNING testSubscribeInGeofence TEST");

		// connect, ping, and disconnect
		Geofence geofence = new Geofence(Location.random(), 20.0);
		BasicClient client = new BasicClient(null, "tcp://localhost", 5559);
		client.sendCONNECT();
		client.sendPINGREQ(geofence.getCircleLocation());
		client.sendSUBSCRIBE(new Topic("test"), geofence);
		client.sendPublish(new Topic("test"), geofence, "Content");
		client.sendDISCONNECT();

		Thread.sleep(1000);

		// check dealer messages
		int messageCount = 5;
		for (int i = 0; i < messageCount; i++) {
			assertEquals("Dealer queue contains wrong number of elements.", messageCount - i,
					client.blockingQueue.size());
			Optional<DealerMessage> dealerMessage = DealerMessage.buildDealerMessage(client.blockingQueue.poll(1, TimeUnit.SECONDS));
			logger.debug(dealerMessage);
			assertTrue("DealerMessage is missing", dealerMessage.isPresent());
			if (i == 3) {
				dealerMessage.ifPresent(message -> {
					assertEquals(ControlPacketType.PUBLISH, message.getControlPacketType());
					PUBLISHPayload payload = message.getPayload().getPUBLISHPayload().get();
					assertEquals("Content", payload.getContent());
				});
			}
		}

		client.tearDown();
		logger.info("FINISHED TEST");
	}

	@SuppressWarnings("ConstantConditions")
	@Test
	public void testSubscribeNotInGeofence() throws InterruptedException, CommunicatorException {
		logger.info("RUNNING testSubscribeInGeofence TEST");

		// connect, ping, and disconnect
		Location location = Location.random();
		Geofence geofence = new Geofence(Location.random(), 20.0);
		BasicClient client = new BasicClient(null, "tcp://localhost", 5559);
		client.sendCONNECT();
		client.sendPINGREQ(Location.random());
		client.sendSUBSCRIBE(new Topic("test"), geofence);
		client.sendPublish(new Topic("test"), geofence, "Content");
		client.sendDISCONNECT();

		Thread.sleep(1000);

		// check dealer messages
		int messageCount = 4;
		for (int i = 0; i < messageCount; i++) {
			assertEquals("Dealer queue contains wrong number of elements.", messageCount - i,
					client.blockingQueue.size());
			Optional<DealerMessage> dealerMessage =
					DealerMessage.buildDealerMessage(client.blockingQueue.poll(1, TimeUnit.SECONDS));
			assertTrue("DealerMessage is missing", dealerMessage.isPresent());
		}

		client.tearDown();
		logger.info("FINISHED TEST");
	}

	// TODO ADD subscribe not in geofence subscribe

}
