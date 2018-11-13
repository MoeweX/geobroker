package de.hasenburg.geofencebroker;

import de.hasenburg.geofencebroker.communication.ZMQProcessManager;
import de.hasenburg.geofencebroker.model.connections.ConnectionManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PublishSubscribeTest {

	private static final Logger logger = LogManager.getLogger();

	ConnectionManager connectionManager;
	ZMQProcessManager processManager;


	// TODO has to be rebuild to later check usage of storage component
//	@SuppressWarnings("Duplicates")
//	@Before
//	public void setUp() {
//		logger.info("Running test setUp");
//
//		connectionManager = new ConnectionManager();
//
//		processManager = new ZMQProcessManager();
//		processManager.runZMQProcess_Broker("tcp://localhost", 5559, "broker");
//		processManager.runZMQProcess_MessageProcessor("message_processor", connectionManager);
//	}
//
//	@After
//	public void tearDown() {
//		logger.info("Running test tearDown.");
//		assertTrue(processManager.tearDown(5000));
//	}
//
//	@SuppressWarnings("ConstantConditions")
//	@Test
//	public void testSubscribeInGeofence() throws InterruptedException, CommunicatorException {
//		logger.info("RUNNING testSubscribeInGeofence TEST");
//
//		// connect, ping, and disconnect
//		Geofence geofence = new Geofence(Location.random(), 20.0);
//		TestClient client = new TestClient(null, "tcp://localhost", 5559);
//		client.sendCONNECT();
//		client.sendPINGREQ(geofence.getCircleLocation()); // subscriber = publisher; booth in geofence
//		client.sendSUBSCRIBE(new Topic("test"), geofence);
//		client.sendPublish(new Topic("test"), geofence, "Content");
//		client.sendDISCONNECT();
//
//		Thread.sleep(1000);
//
//		// check dealer messages
//		int messageCount = 5;
//		for (int i = 0; i < messageCount; i++) {
//			assertEquals("Dealer queue contains wrong number of elements.", messageCount - i,
//					client.blockingQueue.size());
//			Optional<InternalClientMessage> dealerMessage = InternalClientMessage
//					.buildMessage(client.blockingQueue.poll(1, TimeUnit.SECONDS));
//			logger.debug(dealerMessage);
//			assertTrue("InternalClientMessage is missing", dealerMessage.isPresent());
//			if (i == 3) {
//				dealerMessage.ifPresent(message -> {
//					assertEquals(ControlPacketType.PUBLISH, message.getControlPacketType());
//					PUBLISHPayload payload = message.getPayload().getPUBLISHPayload().get();
//					assertEquals("Content", payload.getContent());
//				});
//			}
//			if (i == 4) {
//				dealerMessage.ifPresent(message -> {
//					assertEquals(ControlPacketType.PUBACK, message.getControlPacketType());
//					PUBACKPayload payload = message.getPayload().getPUBACKPayload().get();
//					assertEquals(ReasonCode.Success, payload.getReasonCode());
//				});
//			}
//		}
//
//		client.tearDown();
//		logger.info("FINISHED TEST");
//	}
//
//	@SuppressWarnings("ConstantConditions")
//	@Test
//	public void testSubscriberNotInGeofence() throws InterruptedException, CommunicatorException {
//		logger.info("RUNNING testSubscriberNotInGeofence TEST");
//
//		// subscriber
//		Geofence geofence = new Geofence(Location.random(), 20.0);
//		TestClient clientSubscriber = new TestClient(null, "tcp://localhost", 5559);
//		clientSubscriber.sendCONNECT();
//		clientSubscriber.sendPINGREQ(Location.random()); // subscriber is not in geofence
//		clientSubscriber.sendSUBSCRIBE(new Topic("test"), geofence);
//
//		// publisher
//		TestClient clientPublisher = new TestClient(null, "tcp://localhost", 5559);
//		clientPublisher.sendCONNECT();
//		clientPublisher.sendPINGREQ(geofence.getCircleLocation()); // publisher is in geofence
//		clientPublisher.sendPublish(new Topic("test"), geofence, "Content");
//
//		clientSubscriber.sendDISCONNECT();
//		clientPublisher.sendDISCONNECT();
//
//		Thread.sleep(1000);
//
//		validateNoPublishReceived(clientSubscriber, clientPublisher);
//
//		clientSubscriber.tearDown();
//		clientPublisher.tearDown();
//		logger.info("FINISHED TEST");
//	}
//
//	@SuppressWarnings("ConstantConditions")
//	@Test
//	public void testPublisherNotInGeofence() throws InterruptedException, CommunicatorException {
//		logger.info("RUNNING testPublisherNotInGeofence TEST");
//
//		// subscriber
//		Geofence geofence = new Geofence(Location.random(), 20.0);
//		TestClient clientSubscriber = new TestClient(null, "tcp://localhost", 5559);
//		clientSubscriber.sendCONNECT();
//		clientSubscriber.sendPINGREQ(geofence.getCircleLocation()); // subscriber is in geofence
//		clientSubscriber.sendSUBSCRIBE(new Topic("test"), geofence);
//
//		// publisher
//		TestClient clientPublisher = new TestClient(null, "tcp://localhost", 5559);
//		clientPublisher.sendCONNECT();
//		clientPublisher.sendPINGREQ(Location.random()); // publisher is not in geofence
//		clientPublisher.sendPublish(new Topic("test"), geofence, "Content");
//
//		clientSubscriber.sendDISCONNECT();
//		clientPublisher.sendDISCONNECT();
//
//		Thread.sleep(1000);
//
//		validateNoPublishReceived(clientSubscriber, clientPublisher);
//
//		clientSubscriber.tearDown();
//		clientPublisher.tearDown();
//		logger.info("FINISHED TEST");
//	}
//
//	private void validateNoPublishReceived(TestClient clientSubscriber, TestClient clientPublisher)
//			throws InterruptedException {
//		// check subscriber messages: must not contain "PUBLISH"
//		int subscriberMessageCount = 3;
//		for (int i = 0; i < subscriberMessageCount; i++) {
//			assertEquals("Dealer queue contains wrong number of elements.", subscriberMessageCount - i,
//					clientSubscriber.blockingQueue.size());
//			Optional<InternalClientMessage> dealerMessage =
//					InternalClientMessage.buildMessage(clientSubscriber.blockingQueue.poll(1, TimeUnit.SECONDS));
//			logger.debug(dealerMessage);
//			assertTrue("InternalClientMessage is missing", dealerMessage.isPresent());
//			assertNotEquals(ControlPacketType.PUBLISH, dealerMessage.get().getControlPacketType()); // no publish message
//		}
//
//		// check publisher messages: should contain a PUBACK with no matching subscribers
//		int publisherMessageCount = 3;
//		for (int i = 0; i < publisherMessageCount; i++) {
//			assertEquals("Dealer queue contains wrong number of elements.", publisherMessageCount - i,
//					clientPublisher.blockingQueue.size());
//			Optional<InternalClientMessage> dealerMessage =
//					InternalClientMessage.buildMessage(clientPublisher.blockingQueue.poll(1, TimeUnit.SECONDS));
//			logger.debug(dealerMessage);
//			assertTrue("InternalClientMessage is missing", dealerMessage.isPresent());
//			assertNotEquals(ControlPacketType.PUBLISH, dealerMessage.get().getControlPacketType()); // no publish message
//			if (i == 2) {
//				assertEquals(ControlPacketType.PUBACK, dealerMessage.get().getControlPacketType());
//				assertEquals(ReasonCode.NoMatchingSubscribers, dealerMessage.get().getPayload().getPUBACKPayload().get().getReasonCode());
//			}
//		}
//	}

}
