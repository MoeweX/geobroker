package de.hasenburg.geofencebroker.communication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

/**
 * Contains many helpers to allow the control via ZMQ messages.
 */
public class ZMQControlUtility {

	private static final Logger logger = LogManager.getLogger();

	private static final String CONTROL_CHANNEL = "inproc://zmqControl";

	public enum ZMQControlCommand {
		NONE,
		KILL
	}

	/**
	 *
	 * Fails if someone already bound to the control socket.
	 */
	public static ZMQ.Socket createZMQControlSocket(ZContext context) {
		ZMQ.Socket zmqController = context.createSocket(SocketType.PUB);
		zmqController.bind(CONTROL_CHANNEL);
		return zmqController;
	}

	/**
	 * @param zmqController - the zmq controller socket, IT IS NOT thread-safe
	 */
	public static void sendZMQControlCommand(ZMQ.Socket zmqController, String receiverIdentity, ZMQControlCommand command) {
		ZMsg.newStringMsg(receiverIdentity, command.name()).send(zmqController);
	}

	/**
	 * Creates and registers a subscribe socket that filters all messages except the ones send to the identity of the
	 * zmqControl publisher. New messages can be polled and received via the socket.
	 *
	 * @return - the index in the poller of the subscriber socket
	 */
	public static int connectWithPoller(ZContext context, ZMQ.Poller poller, String identity) {
		ZMQ.Socket zmqControl = context.createSocket(SocketType.SUB);
		zmqControl.connect(CONTROL_CHANNEL);
		zmqControl.subscribe(identity);

		return poller.register(zmqControl, ZMQ.Poller.POLLIN);
	}

	/**
	 * @return the command or NONE if none exists or could not be parsed
	 */
	public static ZMQControlCommand getCommand(ZMQ.Poller poller, int index) {
		if (poller.pollin(index)) {
			ZMsg zMsg = ZMsg.recvMsg(poller.getSocket(index));
			logger.debug("Received control message {}", zMsg);
			try {
				return ZMQControlCommand.valueOf(zMsg.getLast().getString(ZMQ.CHARSET));
			} catch (IllegalArgumentException | NullPointerException e) {
				logger.warn("Received a ZMQControlCommand that cannot be parsed {}", zMsg);
			}
		}
		return ZMQControlCommand.NONE;
	}

}
