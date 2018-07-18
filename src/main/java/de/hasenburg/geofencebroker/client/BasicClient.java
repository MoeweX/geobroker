package de.hasenburg.geofencebroker.client;

import de.hasenburg.geofencebroker.communication.DealerCommunicator;
import de.hasenburg.geofencebroker.model.DealerMessage;
import de.hasenburg.geofencebroker.model.exceptions.CommunicatorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMsg;

import java.util.Iterator;
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
			identifier = "RandomClient-" + random.nextInt(10000);
		}

		dealer = new DealerCommunicator(address, port);
		dealer.init(identifier);
		blockingQueue = new LinkedBlockingDeque<>();
		dealer.startReceiving(blockingQueue);
		logger.info("Started BasicClient.");
	}

	public void tearDown() {
		dealer.stopReceiving();
		logger.info("Stopped BasicClient.");
	}

	public void sendCONNECT() {
		dealer.sendCONNECT();
	}

	public void sendDISCONNECT() {
		dealer.sendDICONNECT();
	}

	public static void main(String[] args) throws CommunicatorException, InterruptedException {
		BasicClient client = new BasicClient(null, "tcp://localhost", 5559);
		client.sendCONNECT();

		Thread.sleep(3000);

		client.sendDISCONNECT();
		for (ZMsg message : client.blockingQueue) {
			logger.info(DealerMessage.buildDealerMessage(message).get().toString());
		}
		Thread.sleep(1000);
		client.tearDown();
	}

}
