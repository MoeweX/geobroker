package de.hasenburg.geofencebroker;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.communication.ReasonCode;
import de.hasenburg.geofencebroker.communication.ZMQProcessManager;
import de.hasenburg.geofencebroker.model.InternalClientMessage;
import de.hasenburg.geofencebroker.model.connections.ConnectionManager;
import de.hasenburg.geofencebroker.model.exceptions.CommunicatorException;
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

	ConnectionManager connectionManager;
	ZMQProcessManager processManager;

	@SuppressWarnings("Duplicates")
	@Before
	public void setUp() {
		logger.info("Running test setUp");

		connectionManager = new ConnectionManager();

		processManager = new ZMQProcessManager();
		processManager.runZMQProcess_Broker("tcp://localhost", 5559, "broker");
		processManager.runZMQProcess_MessageProcessor("message_processor", connectionManager);
	}

	@After
	public void tearDown() {
		logger.info("Running test tearDown.");
		assertTrue(processManager.tearDown(5000));
	}

	@Test
	public void testPingWhileConnected() throws InterruptedException, CommunicatorException {
		logger.info("RUNNING testPingWhileConnected TEST");

		// connect, ping, and disconnect
		TestClient client = new TestClient(null, "tcp://localhost", 5559);
		client.sendCONNECT();

		for (int i = 0; i < 10; i++) {
			client.sendPINGREQ();
			Thread.sleep(100);
		}

		client.sendDISCONNECT();

		// check dealer messages
		for (int i = 0; i < 11; i++) {
			assertEquals("Dealer queue contains wrong number of elements.", 11 - i, client.blockingQueue.size());
			Optional<InternalClientMessage> dealerMessage = InternalClientMessage
					.buildMessage(client.blockingQueue.poll(1, TimeUnit.SECONDS));
			assertTrue("InternalClientMessage is missing", dealerMessage.isPresent());
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
		TestClient client = new TestClient(null, "tcp://localhost", 5559);

		client.sendPINGREQ();

		Optional<InternalClientMessage> dealerMessage = InternalClientMessage
				.buildMessage(client.blockingQueue.poll(1, TimeUnit.SECONDS));
		assertTrue("InternalClientMessage is missing", dealerMessage.isPresent());
		dealerMessage.ifPresent(message -> {
			assertEquals(ControlPacketType.PINGRESP, message.getControlPacketType());
			assertEquals(ReasonCode.NotConnected, message.getPayload().getPINGRESPPayload().get().getReasonCode());
		});

		client.tearDown();
		logger.info("FINISHED TEST");
	}

}
