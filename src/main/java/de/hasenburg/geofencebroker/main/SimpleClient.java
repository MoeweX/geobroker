package de.hasenburg.geofencebroker.main;

import de.hasenburg.geofencebroker.communication.DealerCommunicator;
import de.hasenburg.geofencebroker.model.DealerMessage;
import de.hasenburg.geofencebroker.model.exceptions.CommunicatorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.zeromq.ZMsg;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class SimpleClient {

	private static final Logger logger = LogManager.getLogger();

	private DealerCommunicator dealer;
	public BlockingQueue<ZMsg> blockingQueue;

	public SimpleClient(@Nullable String identifier, String address, int port) {
		if (identifier == null) {
			Random random = new Random();
			identifier = "SimpleClient-" + System.nanoTime();
		}

		dealer = new DealerCommunicator(address, port);
		dealer.init(identifier);
		blockingQueue = new LinkedBlockingDeque<>();
		logger.info("Created client {}", identifier);
	}

	public void startReceiving() throws CommunicatorException {
		dealer.startReceiving(blockingQueue);
		logger.info("Receiving with client " + dealer.getIdentity());
	}

	public String getIdentity() {
		return dealer.getIdentity();
	}

	public int getQueueSize() {
		return blockingQueue.size();
	}

	public void tearDown() {
		dealer.tearDown();
		logger.info("Stopped client .");
	}

	public void sendDealerMessage(DealerMessage message) {
		dealer.sendDealerMessage(message);
	}

	public Optional<DealerMessage> getNextDealerMessage() {
		return DealerMessage.buildDealerMessage(blockingQueue.poll());
	}

}
