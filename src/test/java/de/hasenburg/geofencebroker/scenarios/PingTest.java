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
import de.hasenburg.geofencebroker.model.payload.PINGREQPayload;
import de.hasenburg.geofencebroker.model.spatial.Location;
import de.hasenburg.geofencebroker.model.storage.TopicAndGeofenceMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

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

		processManager = new ZMQProcessManager();
		processManager.runZMQProcess_Broker("tcp://localhost", 5559, "broker");
		processManager.runZMQProcess_MessageProcessor("message_processor", clientDirectory, topicAndGeofenceMapper);
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
		assertEquals(ReasonCode.NotConnected, internalClientMessage.getPayload().getPINGRESPPayload().get().getReasonCode());
	}

}
