package de.hasenburg.geobroker.server.scenarios;

import de.hasenburg.geobroker.client.communication.InternalClientMessage;
import de.hasenburg.geobroker.client.main.SimpleClient;
import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.communication.ZMQProcessManager;
import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.message.ReasonCode;
import de.hasenburg.geobroker.commons.model.message.payloads.CONNECTPayload;
import de.hasenburg.geobroker.commons.model.message.payloads.DISCONNECTPayload;
import de.hasenburg.geobroker.commons.model.message.payloads.PINGREQPayload;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import de.hasenburg.geobroker.server.main.Configuration;
import de.hasenburg.geobroker.server.main.server.SingleGeoBrokerServerLogic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("ConstantConditions")
public class PingTest {

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

	@Test
	public void compareSerial() {
		double time1 = System.nanoTime();
		// connect, ping, and disconnect
		SimpleClient client = new SimpleClient(null, "localhost", 5559, clientProcessManager);
		client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.CONNECT,
				new CONNECTPayload(Location.random())));
		for (int i = 0; i < 100; i++) {
			client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.PINGREQ,
					new PINGREQPayload(Location.random())));
		}

		client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.DISCONNECT,
				new DISCONNECTPayload(ReasonCode.NormalDisconnection)));

		for (int i = 0; i < 101; i++) {
			InternalClientMessage internalClientMessage = client.receiveInternalClientMessage();
			if (i == 0) {
				assertEquals(ControlPacketType.CONNACK, internalClientMessage.getControlPacketType());
			} else {
				assertEquals(ControlPacketType.PINGRESP, internalClientMessage.getControlPacketType());
				assertEquals(ReasonCode.LocationUpdated,
						internalClientMessage.getPayload().getPINGRESPPayload().get().getReasonCode());
			}
		}
		double time2 = System.nanoTime();
		logger.info("Messages took {}s", (time2 - time1)/1000000000);
	}

	@Test
	public void testPingWhileConnected() {
		// connect, ping, and disconnect
		SimpleClient client = new SimpleClient(null, "localhost", 5559, clientProcessManager);
		client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.CONNECT,
				new CONNECTPayload(Location.random())));
		for (int i = 0; i < 10; i++) {
			client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.PINGREQ,
					new PINGREQPayload(Location.random())));
			Utility.sleepNoLog(100, 0);
		}

		client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.DISCONNECT,
				new DISCONNECTPayload(ReasonCode.NormalDisconnection)));

		// check dealer messages
		for (int i = 0; i < 11; i++) {
			InternalClientMessage internalClientMessage = client.receiveInternalClientMessage();

			if (i == 0) {
				assertEquals(ControlPacketType.CONNACK, internalClientMessage.getControlPacketType());
			} else {
				assertEquals(ControlPacketType.PINGRESP, internalClientMessage.getControlPacketType());
				assertEquals(ReasonCode.LocationUpdated,
						internalClientMessage.getPayload().getPINGRESPPayload().get().getReasonCode());
			}
		}
	}

	@SuppressWarnings("ConstantConditions")
	@Test
	public void testPingWhileNotConnected() {
		SimpleClient client = new SimpleClient(null, "localhost", 5559, clientProcessManager);

		client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.PINGREQ,
				new PINGREQPayload(Location.random())));

		InternalClientMessage internalClientMessage = client.receiveInternalClientMessage();

		assertEquals(ControlPacketType.PINGRESP, internalClientMessage.getControlPacketType());
		assertEquals(ReasonCode.NotConnected,
				internalClientMessage.getPayload().getPINGRESPPayload().get().getReasonCode());
	}

}
