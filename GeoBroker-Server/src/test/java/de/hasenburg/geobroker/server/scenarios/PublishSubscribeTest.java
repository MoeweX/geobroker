package de.hasenburg.geobroker.server.scenarios;

import de.hasenburg.geobroker.commons.communication.ZMQProcessManager;
import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.message.ReasonCode;
import de.hasenburg.geobroker.server.communication.ZMQProcessStarter;
import de.hasenburg.geobroker.server.distribution.BrokerAreaManager;
import de.hasenburg.geobroker.server.main.Configuration;
import de.hasenburg.geobroker.client.main.SimpleClient;
import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.client.communication.InternalClientMessage;
import de.hasenburg.geobroker.commons.model.message.Topic;
import de.hasenburg.geobroker.server.matching.SingleGeoBrokerMatchingLogic;
import de.hasenburg.geobroker.server.storage.client.ClientDirectory;
import de.hasenburg.geobroker.commons.model.message.payloads.CONNECTPayload;
import de.hasenburg.geobroker.commons.model.message.payloads.PUBLISHPayload;
import de.hasenburg.geobroker.commons.model.message.payloads.SUBSCRIBEPayload;
import de.hasenburg.geobroker.commons.model.spatial.Geofence;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class PublishSubscribeTest {

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

		SingleGeoBrokerMatchingLogic matchingLogic =
				new SingleGeoBrokerMatchingLogic(clientDirectory, topicAndGeofenceMapper);

		processManager = new ZMQProcessManager();
		ZMQProcessStarter.runZMQProcess_Server(processManager, "tcp://localhost", 5559, "broker");
		ZMQProcessStarter.runZMQProcess_MessageProcessor(processManager, "message_processor", matchingLogic);
	}

	@After
	public void tearDown() {
		logger.info("Running test tearDown.");
		assertTrue(processManager.tearDown(5000));
	}

	@SuppressWarnings({"ConstantConditions", "OptionalGetWithoutIsPresent"})
	@Test
	public void testSubscribeInGeofence() {
		logger.info("RUNNING testSubscribeInGeofence TEST");

		// connect, ping, and disconnect
		Location l = Location.random();
		Geofence g = Geofence.circle(l, 0.4);
		Topic t = new Topic("test");

		SimpleClient client = new SimpleClient(null, "tcp://localhost", 5559, processManager);
		client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.CONNECT, new CONNECTPayload(l)));
		client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.SUBSCRIBE,
																   new SUBSCRIBEPayload(t, g)));
		client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.PUBLISH,
																   new PUBLISHPayload(t, g, "Content")));

		Utility.sleepNoLog(500, 0);

		// validate payloads
		InternalClientMessage message = client.receiveInternalClientMessage();
		assertEquals(ControlPacketType.CONNACK, message.getControlPacketType());

		message = client.receiveInternalClientMessage();
		assertEquals(ControlPacketType.SUBACK, message.getControlPacketType());

		message = client.receiveInternalClientMessage();
		assertEquals(ControlPacketType.PUBLISH, message.getControlPacketType());
		assertEquals("Content", message.getPayload().getPUBLISHPayload().get().getContent());
		logger.info("Received published message {}", message);

		message = client.receiveInternalClientMessage();
		assertEquals(ControlPacketType.PUBACK, message.getControlPacketType());
		assertEquals(ReasonCode.Success, message.getPayload().getPUBACKPayload().get().getReasonCode());
	}

	@SuppressWarnings("ConstantConditions")
	@Test
	public void testSubscriberNotInGeofence() {
		// subscriber
		Location l = Location.random();
		Geofence g = Geofence.circle(l, 0.4);
		Topic t = new Topic("test");

		SimpleClient clientSubscriber = new SimpleClient(null, "tcp://localhost", 5559, processManager);
		clientSubscriber.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.CONNECT,
																			 new CONNECTPayload(Location.random()))); // subscriber not in geofence
		clientSubscriber.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.SUBSCRIBE,
																			 new SUBSCRIBEPayload(t, g)));

		// publisher
		SimpleClient clientPublisher = new SimpleClient(null, "tcp://localhost", 5559, processManager);
		clientPublisher.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.CONNECT,
																			new CONNECTPayload(l))); // publisher is in geofence
		clientPublisher.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.PUBLISH,
																			new PUBLISHPayload(t, g, "Content")));

		Utility.sleepNoLog(500, 0);

		validateNoPublishReceived(clientSubscriber, clientPublisher);
	}

	@SuppressWarnings("ConstantConditions")
	@Test
	public void testPublisherNotInGeofence() {
		// subscriber
		Location l = Location.random();
		Geofence g = Geofence.circle(l, 0.4);
		Topic t = new Topic("test");

		SimpleClient clientSubscriber = new SimpleClient(null, "tcp://localhost", 5559, processManager);
		clientSubscriber.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.CONNECT,
																			 new CONNECTPayload(l))); // subscriber is in geofence
		clientSubscriber.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.SUBSCRIBE,
																			 new SUBSCRIBEPayload(t, g)));

		// publisher
		SimpleClient clientPublisher = new SimpleClient(null, "tcp://localhost", 5559, processManager);
		clientPublisher.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.CONNECT,
																			new CONNECTPayload(Location.random()))); // publisher is not in geofence
		clientPublisher.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.PUBLISH,
																			new PUBLISHPayload(t, g, "Content")));

		Utility.sleepNoLog(500, 0);

		validateNoPublishReceived(clientSubscriber, clientPublisher);
	}

	@SuppressWarnings("OptionalGetWithoutIsPresent")
	private void validateNoPublishReceived(SimpleClient clientSubscriber, SimpleClient clientPublisher) {
		// check subscriber messages: must not contain "PUBLISH"
		int subscriberMessageCount = 2;
		for (int i = 0; i < subscriberMessageCount; i++) {
			assertNotEquals(ControlPacketType.PUBLISH,
							clientSubscriber
									.receiveInternalClientMessage()
									.getControlPacketType()); // no publish message
		}

		// check publisher messages: should contain a PUBACK with no matching subscribers
		int publisherMessageCount = 2;
		for (int i = 0; i < publisherMessageCount; i++) {
			InternalClientMessage message = clientPublisher.receiveInternalClientMessage();
			assertNotEquals(ControlPacketType.PUBLISH, message.getControlPacketType()); // no publish message
			if (i == 1) {
				assertEquals(ControlPacketType.PUBACK, message.getControlPacketType());
				assertEquals(ReasonCode.NoMatchingSubscribers,
							 message.getPayload().getPUBACKPayload().get().getReasonCode());
			}
		}
	}

}
