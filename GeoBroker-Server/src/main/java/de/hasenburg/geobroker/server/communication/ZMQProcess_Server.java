package de.hasenburg.geobroker.server.communication;

import de.hasenburg.geobroker.commons.communication.ZMQControlUtility;
import de.hasenburg.geobroker.commons.communication.ZMQProcess;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import java.util.Arrays;
import java.util.List;

class ZMQProcess_Server extends ZMQProcess {

	private static final Logger logger = LogManager.getLogger();

	// Address and port of server frontend
	private String address;
	private int port;

	// Address of server backend
	static final String SERVER_INPROC_ADDRESS = "inproc://server";

	// socket indices
	private final int FRONTEND_INDEX = 0;
	private final int BACKEND_INDEX = 1;

	ZMQProcess_Server(String address, int port, String identity) {
		super(identity);
		this.address = address;
		this.port = port;
	}

	@Override
	protected List<Socket> bindAndConnectSockets(ZContext context) {
		Socket[] socketArray = new Socket[2];

		Socket frontend = context.createSocket(SocketType.ROUTER);
		frontend.bind(address + ":" + port);
		frontend.setIdentity(identity.getBytes());
		frontend.setSendTimeOut(1);
		socketArray[FRONTEND_INDEX] = frontend;

		Socket backend = context.createSocket(SocketType.DEALER);
		backend.bind(SERVER_INPROC_ADDRESS);
		backend.setSendTimeOut(1);
		socketArray[BACKEND_INDEX] = backend;

		return Arrays.asList(socketArray);
	}

	@Override
	protected void processZMQControlCommandOtherThanKill(ZMQControlUtility.ZMQControlCommand zmqControlCommand) {
		// no other commands are of interest
	}

	@Override
	protected void processZMsg(int socketIndex, ZMsg msg) {
		switch (socketIndex) {
			case BACKEND_INDEX:
				if (!msg.send(sockets.get(FRONTEND_INDEX))) {
					logger.warn("Dropping response to client as HWM reached.");
				}
				break;
			case FRONTEND_INDEX:
				if (!msg.send(sockets.get(BACKEND_INDEX))) {
					logger.warn("Dropping client request as HWM reached.");
				}
				break;
			default:
				logger.error("Cannot process message for socket at index {}, as this index is not known.", socketIndex);
		}
	}

	@Override
	protected void shutdownCompleted() {
		logger.info("Shut down ZMQProcess_Server {}", identity);
	}

}
