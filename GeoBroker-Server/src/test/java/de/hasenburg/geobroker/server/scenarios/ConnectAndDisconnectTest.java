package de.hasenburg.geobroker.server.scenarios;

import de.hasenburg.geobroker.client.communication.InternalClientMessage;
import de.hasenburg.geobroker.client.main.SimpleClient;
import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.communication.ZMQProcessManager;
import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.message.ReasonCode;
import de.hasenburg.geobroker.commons.model.message.payloads.CONNECTPayload;
import de.hasenburg.geobroker.commons.model.message.payloads.DISCONNECTPayload;
import de.hasenburg.geobroker.commons.model.spatial.Geofence;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import de.hasenburg.geobroker.server.communication.ZMQProcessStarter;
import de.hasenburg.geobroker.server.distribution.BrokerArea;
import de.hasenburg.geobroker.server.distribution.BrokerAreaManager;
import de.hasenburg.geobroker.server.main.Configuration;
import de.hasenburg.geobroker.server.matching.DisGBAtSubscriberMatchingLogic;
import de.hasenburg.geobroker.server.matching.SingleGeoBrokerMatchingLogic;
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper;
import de.hasenburg.geobroker.server.storage.client.ClientDirectory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class ConnectAndDisconnectTest {

	private static final Logger logger = LogManager.getLogger();

	private BrokerAreaManager brokerAreaManager;
	private ClientDirectory clientDirectory;
	private ZMQProcessManager processManager;

	@SuppressWarnings("Duplicates")
	@Before
	public void setUp() {
		logger.info("Running test setUp");

		clientDirectory = new ClientDirectory();
		TopicAndGeofenceMapper topicAndGeofenceMapper = new TopicAndGeofenceMapper(new Configuration());

		brokerAreaManager = new BrokerAreaManager("broker");
		brokerAreaManager.setup_DefaultFile();

		DisGBAtSubscriberMatchingLogic matchingLogic =
				new DisGBAtSubscriberMatchingLogic(clientDirectory, topicAndGeofenceMapper, brokerAreaManager);

		processManager = new ZMQProcessManager();
		ZMQProcessStarter.runZMQProcess_Server(processManager, "tcp://localhost", 5559, "broker");
		ZMQProcessStarter.runZMQProcess_MessageProcessor(processManager, "message_processor", matchingLogic);

		assertEquals(0, clientDirectory.getNumberOfClients());
	}

	@After
	public void tearDown() {
		logger.info("Running test tearDown.");
		assertTrue(processManager.tearDown(5000));
	}

	@Test
	public void testOneClient() {
		SimpleClient client = new SimpleClient(null, "tcp://localhost", 5559, processManager);

		// connect
		client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.CONNECT,
																   new CONNECTPayload(Location.random())));
		assertEquals(ControlPacketType.CONNACK, client.receiveInternalClientMessage().getControlPacketType());

		// check whether client exists
		assertEquals(1, clientDirectory.getNumberOfClients());

		// disconnect
		client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.DISCONNECT,
																   new DISCONNECTPayload(ReasonCode.NormalDisconnection)));

		// check whether disconnected and no more messages received
		Utility.sleepNoLog(1, 0);
		assertEquals(0, clientDirectory.getNumberOfClients());
	}

	@Test
	public void testMultipleClients() {
		List<SimpleClient> clients = new ArrayList<>();
		int activeConnections = 10;
		Random random = new Random();

		// create clients
		for (int i = 0; i < activeConnections; i++) {
			SimpleClient client = new SimpleClient(null, "tcp://localhost", 5559, processManager);
			clients.add(client);
		}

		// send connects and randomly also disconnect
		for (SimpleClient client : clients) {
			client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.CONNECT,
																	   new CONNECTPayload(Location.random())));
			if (random.nextBoolean()) {
				client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.DISCONNECT,
																		   new DISCONNECTPayload(ReasonCode.NormalDisconnection)));
				activeConnections--;
			}
		}

		// check acknowledgements
		for (SimpleClient client : clients) {
			assertEquals(ControlPacketType.CONNACK, client.receiveInternalClientMessage().getControlPacketType());
		}

		Utility.sleepNoLog(1, 0);
		// check number of active clients
		assertEquals("Wrong number of active clients", activeConnections, clientDirectory.getNumberOfClients());
		logger.info("{} out of {} clients were active, so everything fine", activeConnections, 10);
	}

	@Test
	public void testNotResponsibleClient() {
		brokerAreaManager.updateOwnBrokerArea(new BrokerArea(brokerAreaManager.getOwnBrokerInfo(),
															 Geofence.circle(new Location(0, 0), 10)));

		SimpleClient client = new SimpleClient(null, "tcp://localhost", 5559, processManager);

		// connect
		client.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.CONNECT,
																   new CONNECTPayload(new Location(30, 30))));
		InternalClientMessage response = client.receiveInternalClientMessage();
		assertEquals(ControlPacketType.DISCONNECT, response.getControlPacketType());
		assertEquals(ReasonCode.WrongBroker, response.getPayload().getDISCONNECTPayload().get().getReasonCode());

		// check whether client exists
		assertEquals(0, clientDirectory.getNumberOfClients());
	}

}
