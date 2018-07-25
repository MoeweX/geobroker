package de.hasenburg.geofencebroker.client;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.communication.DealerCommunicator;
import de.hasenburg.geofencebroker.communication.ReasonCode;
import de.hasenburg.geofencebroker.model.*;
import de.hasenburg.geofencebroker.model.exceptions.CommunicatorException;
import de.hasenburg.geofencebroker.model.geofence.Geofence;
import de.hasenburg.geofencebroker.model.payload.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMsg;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class BasicClient {

	private static final Logger logger = LogManager.getLogger();

	private DealerCommunicator dealer;
	public BlockingQueue<ZMsg> blockingQueue;

	public BasicClient(String identifier, String address, int port) throws CommunicatorException {
		if (identifier == null) {
			Random random = new Random();
			identifier = "BasicClient-" + System.nanoTime();
		}

		dealer = new DealerCommunicator(address, port);
		dealer.init(identifier);
		blockingQueue = new LinkedBlockingDeque<>();
		dealer.startReceiving(blockingQueue);
		logger.info("Started BasicClient " + getIdentity());
	}

	public void tearDown() {
		dealer.tearDown();
		logger.info("Stopped BasicClient.");
	}

	public String getIdentity() {
		return dealer.getIdentity();
	}

	public void sendCONNECT() {
		logger.trace("Connecting with client " + getIdentity());
		dealer.sendDealerMessage(new DealerMessage(ControlPacketType.CONNECT, new CONNECTPayload()));
	}

	public void sendPINGREQ() {
		logger.trace("Pinging with client " + getIdentity());
		dealer.sendDealerMessage(new DealerMessage(ControlPacketType.PINGREQ, new PINGREQPayload(Location.random())));
	}

	public void sendPINGREQ(Location location) {
		logger.trace("Pinging with client " + getIdentity());
		dealer.sendDealerMessage(new DealerMessage(ControlPacketType.PINGREQ, new PINGREQPayload(location)));
	}

	public void sendDISCONNECT() {
		logger.trace("Disconnecting with client " + getIdentity());
		dealer.sendDealerMessage(new DealerMessage(ControlPacketType.DISCONNECT, new DISCONNECTPayload(ReasonCode.NormalDisconnection)));
	}

	public void sendSUBSCRIBE(Topic topic) {
		logger.trace("Subscribing with client {} to topic {}", getIdentity(), topic);
		Geofence geofence = new Geofence(Location.random(), 20.0);
		SUBSCRIBEPayload payload = new SUBSCRIBEPayload(topic, geofence);
		dealer.sendDealerMessage(new DealerMessage(ControlPacketType.SUBSCRIBE, payload));
	}

	public void sendSUBSCRIBE(Topic topic, Geofence geofence) {
		logger.trace("Subscribing with client {} to topic {}", getIdentity(), topic);
		dealer.sendDealerMessage(new DealerMessage(ControlPacketType.SUBSCRIBE, new SUBSCRIBEPayload(topic, geofence)));
	}

	public void sendPublish(Topic topic, String content) {
		logger.trace("Publishing with client {} to topic {}: {}", getIdentity(), topic, content);
		Geofence geofence = new Geofence(Location.random(), 20.0);
		dealer.sendDealerMessage(new DealerMessage(ControlPacketType.PUBLISH, new PUBLISHPayload(topic, geofence, content)));
	}

	public void sendPublish(Topic topic, Geofence geofence, String content) {
		logger.trace("Publishing with client {} to topic {}: {}", getIdentity(), topic, content);
		dealer.sendDealerMessage(new DealerMessage(ControlPacketType.PUBLISH, new PUBLISHPayload(topic, geofence, content)));
	}

	public static void main(String[] args) throws CommunicatorException, InterruptedException, IOException {
		BasicClient client = new BasicClient(null, "tcp://localhost", 5559);
		client.sendCONNECT();
		Location l = Location.random();
		client.sendPINGREQ(l);
		client.sendSUBSCRIBE(new Topic("Test Topic"));
		client.sendPublish(new Topic("Test Topic"), new Geofence(l, 20.0), "My message content");

		System.in.read();

		client.sendDISCONNECT();
		for (ZMsg message : client.blockingQueue) {
			logger.info(DealerMessage.buildDealerMessage(message).get().toString());
		}
		Thread.sleep(1000);
		client.tearDown();
	}

}
