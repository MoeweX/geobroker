package de.hasenburg.geobroker.server.scenarios;

import de.hasenburg.geobroker.client.communication.InternalClientMessage;
import de.hasenburg.geobroker.client.main.SimpleClient;
import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.communication.ZMQProcessManager;
import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.message.ReasonCode;
import de.hasenburg.geobroker.commons.model.message.Topic;
import de.hasenburg.geobroker.commons.model.message.payloads.CONNECTPayload;
import de.hasenburg.geobroker.commons.model.message.payloads.PUBLISHPayload;
import de.hasenburg.geobroker.commons.model.message.payloads.SUBSCRIBEPayload;
import de.hasenburg.geobroker.commons.model.message.payloads.UNSUBSCRIBEPayload;
import de.hasenburg.geobroker.commons.model.spatial.Geofence;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import de.hasenburg.geobroker.server.main.Configuration;
import de.hasenburg.geobroker.server.main.server.SingleGeoBrokerServerLogic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class SubscribeUnsubscribeTest {

	private static final Logger logger = LogManager.getLogger();

	private SingleGeoBrokerServerLogic serverLogic;
	private ZMQProcessManager clientProcessManager;

	@SuppressWarnings("Duplicates")
	@Before
	public void setUp() {
		logger.info("Running test setUp");

		serverLogic = new SingleGeoBrokerServerLogic();
		serverLogic.loadConfiguration(new Configuration());
		serverLogic.initializeFields();
		serverLogic.startServer();

		clientProcessManager = new ZMQProcessManager();
	}

	@After
	public void tearDown() {
		logger.info("Running test tearDown.");
		clientProcessManager.tearDown(2000);
		serverLogic.cleanUp();
	}

	@SuppressWarnings({"OptionalGetWithoutIsPresent"})
	@Test
	public void testSubscribeUnsubscribe() {
		// connect, ping, and disconnect
		String cI = "testClient";
		Location l = Location.random();
		Geofence g = Geofence.circle(l, 0.4);
		Topic t = new Topic("test");

		SimpleClient client = new SimpleClient(cI, "localhost", 5559, clientProcessManager);
		client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.CONNECT, new CONNECTPayload(l)));
		client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.SUBSCRIBE,
				new SUBSCRIBEPayload(t, g)));

		Utility.sleepNoLog(500, 0);

		// validate payloads
		InternalClientMessage message = client.receiveInternalClientMessage();
		assertEquals(ControlPacketType.CONNACK, message.getControlPacketType());

		message = client.receiveInternalClientMessage();
		assertEquals(ControlPacketType.SUBACK, message.getControlPacketType());
		assertEquals(1, serverLogic.getClientDirectory().getCurrentClientSubscriptions(cI));
		logger.info("Client has successfully subscribed");

		// Unsubscribe
		client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.UNSUBSCRIBE,
				new UNSUBSCRIBEPayload(t, g)));

		message = client.receiveInternalClientMessage();
		assertEquals(ControlPacketType.UNSUBACK, message.getControlPacketType());
		assertEquals(ReasonCode.Success, message.getPayload().getUNSUBACKPayload().get().getReasonCode());
		assertEquals(0, serverLogic.getClientDirectory().getCurrentClientSubscriptions(cI));
	}

	@SuppressWarnings({"OptionalGetWithoutIsPresent"})
	@Test
	public void testUnsubscribeNotConnected() {
		// connect, ping, and disconnect
		Location l = Location.random();
		Geofence g = Geofence.circle(l, 0.4);
		Topic t = new Topic("test");
		String cI = "testClient";

		SimpleClient client = new SimpleClient(cI, "localhost", 5559, clientProcessManager);

		// Unsubscribe
		client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.UNSUBSCRIBE,
				new UNSUBSCRIBEPayload(t, g)));

		InternalClientMessage message = client.receiveInternalClientMessage();
		assertEquals(ControlPacketType.UNSUBACK, message.getControlPacketType());
		assertEquals(ReasonCode.NoSubscriptionExisted, message.getPayload().getUNSUBACKPayload().get().getReasonCode());
		assertEquals(0, serverLogic.getClientDirectory().getCurrentClientSubscriptions(cI));
	}

}
