package de.hasenburg.geofencebroker.communication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

class ZMQProcess_Broker implements Runnable {

	private static final Logger logger = LogManager.getLogger();
	private static final String PROCESSING_BACKEND = "inproc://backend";
	private static final int TIMEOUT_SECONDS = 10; // logs when not received in time, but repeats

	// Address and port of broker
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
		ZMQ.Socket frontend = context.createSocket(ZMQ.ROUTER);
		frontend.bind(address + ":" + port);
		frontend.setIdentity(identity.getBytes());

		ZMQ.Socket backend = context.createSocket(ZMQ.DEALER);
		backend.bind(PROCESSING_BACKEND);

		ZMQ.Poller poller = context.createPoller(1);
		int zmqControlIndex = ZMQControlUtility.connectWithPoller(context, poller, identity);
		poller.register(frontend, ZMQ.Poller.POLLIN);
		poller.register(backend, ZMQ.Poller.POLLIN);

		while (!Thread.currentThread().isInterrupted()) {

			logger.trace("Broker waiting {}s for a message", TIMEOUT_SECONDS);
			poller.poll(TIMEOUT_SECONDS * 1000);

			if (poller.pollin(zmqControlIndex)) {
				if (ZMQControlUtility.getCommand(poller, zmqControlIndex)
						.equals(ZMQControlUtility.ZMQControlCommand.KILL)) {
					break;
				}
			} else if (poller.pollin(1)) {
				ZMsg msg = ZMsg.recvMsg(frontend);
				msg.send(backend);
			} else if (poller.pollin(2)) {
				ZMsg msg = ZMsg.recvMsg(backend);
				msg.send(frontend);
			} else {
				logger.debug("Did not receive a message for {}s", TIMEOUT_SECONDS);
			}

		} // end while loop

		context.destroySocket(frontend);
		context.destroySocket(backend);
		logger.info("Shut down ZMQProcess_Broker, frontend and backend sockets were destroyed.");
	}

}
