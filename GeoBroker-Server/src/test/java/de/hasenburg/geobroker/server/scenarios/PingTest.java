package de.hasenburg.geobroker.server.scenarios;

import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.message.ReasonCode;
import de.hasenburg.geobroker.commons.communication.ZMQProcessManager;
import de.hasenburg.geobroker.server.communication.ZMQProcessStarter;
import de.hasenburg.geobroker.server.distribution.BrokerAreaManager;
import de.hasenburg.geobroker.server.main.Configuration;
import de.hasenburg.geobroker.client.main.SimpleClient;
import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.client.communication.InternalClientMessage;
import de.hasenburg.geobroker.server.matching.SingleGeoBrokerMatchingLogic;
import de.hasenburg.geobroker.server.storage.client.ClientDirectory;
import de.hasenburg.geobroker.commons.exceptions.CommunicatorException;
import de.hasenburg.geobroker.commons.model.message.payloads.CONNECTPayload;
import de.hasenburg.geobroker.commons.model.message.payloads.DISCONNECTPayload;
import de.hasenburg.geobroker.commons.model.message.payloads.PINGREQPayload;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("ConstantConditions")
public class PingTest {

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

	@Test
	public void testPingWhileConnected() throws InterruptedException, CommunicatorException {
		// connect, ping, and disconnect
		SimpleClient client = new SimpleClient(null, "tcp://localhost", 5559, processManager);
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
	public void testPingWhileNotConnected() throws InterruptedException, CommunicatorException {
		SimpleClient client = new SimpleClient(null, "tcp://localhost", 5559, processManager);

		client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.PINGREQ,
																   new PINGREQPayload(Location.random())));

		InternalClientMessage internalClientMessage = client.receiveInternalClientMessage();

		assertEquals(ControlPacketType.PINGRESP, internalClientMessage.getControlPacketType());
		assertEquals(ReasonCode.NotConnected,
					 internalClientMessage.getPayload().getPINGRESPPayload().get().getReasonCode());
	}

}
