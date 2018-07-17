package de.hasenburg.geofencebroker.communication;

import de.hasenburg.geofencebroker.exceptions.CommunicatorException;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class RouterCommunicator extends ZMQCommunicator {

	public RouterCommunicator(String address, int port) {
		super(address, port);
	}

	@Override
	public void init(String identity) {
		context = ZMQ.context(1);
		socket = context.socket(ZMQ.ROUTER);

		if (identity != null) { socket.setIdentity(identity.getBytes()); }
		// socket.setRouterMandatory(true);

		socket.bind(address + ":" + port);
	}

	public static void main(String[] args) throws CommunicatorException, InterruptedException {
		RouterCommunicator router = new RouterCommunicator("tcp://localhost", 5559);
		router.init(null);
		BlockingQueue<ZMsg> blockingQueue = new LinkedBlockingDeque<>();
		router.startReceiving(blockingQueue);
	}

}
