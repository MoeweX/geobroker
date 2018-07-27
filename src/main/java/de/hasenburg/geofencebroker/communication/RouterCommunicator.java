package de.hasenburg.geofencebroker.communication;

import de.hasenburg.geofencebroker.model.RouterMessage;
import de.hasenburg.geofencebroker.model.exceptions.CommunicatorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Should not be used in situations with multiple messages as uses the not threadsafe socket in different threads.
 */
public class RouterCommunicator extends ZMQCommunicator {

	private static final Logger logger = LogManager.getLogger();

	public RouterCommunicator(String address, int port) {
		super(address, port, logger);
	}

	@Override
	public void init(String identity) {
		context = ZMQ.context(1);
		socket = context.socket(ZMQ.ROUTER);

		if (identity != null) { socket.setIdentity(identity.getBytes()); }
		// socket.setRouterMandatory(true);

		socket.bind(address + ":" + port);
	}

	public void sendRouterMessage(RouterMessage message) {
		sendMessage(message.getZMsg());
	}

	public static void main(String[] args) throws CommunicatorException {
		RouterCommunicator router = new RouterCommunicator("tcp://localhost", 5559);
		router.init(null);
		BlockingQueue<ZMsg> blockingQueue = new LinkedBlockingDeque<>();
		router.startReceiving(blockingQueue);
	}

}
