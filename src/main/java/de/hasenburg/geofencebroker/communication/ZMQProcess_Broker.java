package de.hasenburg.geofencebroker.communication;

import de.hasenburg.geofencebroker.main.BenchmarkHelper;
import de.hasenburg.geofencebroker.main.LoadTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

class ZMQProcess_Broker implements Runnable {

	private static final Logger logger = LogManager.getLogger();
	public static final String BROKER_PROCESSING_BACKEND = "inproc://backend";
	private static final int TIMEOUT_SECONDS = 10; // logs when not received in time, but repeats

	// Address and port of broker frontend
	private String address;
	private int port;
	private String identity;

	// Socket and context
	private ZContext context;

	protected ZMQProcess_Broker(String address, int port, String identity, ZContext context) {
		this.address = address;
		this.port = port;
		this.identity = identity;

		this.context = context;
	}

	@Override
	public void run() {
		Thread.currentThread().setName(identity);

		ZMQ.Socket frontend = context.createSocket(SocketType.ROUTER);
		frontend.bind(address + ":" + port);
		frontend.setIdentity(identity.getBytes());
		frontend.setSendTimeOut(1);

		ZMQ.Socket backend = context.createSocket(SocketType.DEALER);
		backend.bind(BROKER_PROCESSING_BACKEND);
		backend.setSendTimeOut(1);

		ZMQ.Poller poller = context.createPoller(1);
		int zmqControlIndex = ZMQControlUtility.connectWithPoller(context, poller, identity);
		poller.register(frontend, ZMQ.Poller.POLLIN);
		poller.register(backend, ZMQ.Poller.POLLIN);

		while (!Thread.currentThread().isInterrupted()) {
			long time = System.nanoTime();
			logger.trace("Waiting {}s for a message", TIMEOUT_SECONDS);
			poller.poll(TIMEOUT_SECONDS * 1000);

			if (poller.pollin(zmqControlIndex)) {
				if (ZMQControlUtility.getCommand(poller, zmqControlIndex)
						.equals(ZMQControlUtility.ZMQControlCommand.KILL)) {
					break;
				}
			} else if (poller.pollin(2)) { // first check outgoing messages to clients
				ZMsg msg = ZMsg.recvMsg(backend);
				if (!msg.send(frontend)) {
					logger.warn("Dropping response to client as HWM reached.");
				}
			} else if (poller.pollin(1)) { // only accept new, if no outgoing messages pending
				ZMsg msg = ZMsg.recvMsg(frontend);
				if (!msg.send(backend)) {
					logger.warn("Dropping client request as HWM reached.");
				}
			}
			BenchmarkHelper.addEntry("brokerForward", System.nanoTime() - time);
		} // end while loop

		// sub control socket (might be optional, kill nevertheless)
		context.destroySocket(poller.getSocket(0));

		// other sockets
		context.destroySocket(frontend);
		context.destroySocket(backend);
		logger.info("Shut down ZMQProcess_Broker, frontend and backend sockets were destroyed.");
	}

}
