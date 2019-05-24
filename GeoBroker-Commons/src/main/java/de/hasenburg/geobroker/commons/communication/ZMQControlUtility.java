package de.hasenburg.geobroker.commons.communication;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
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
		KILL,
		SEND_ZMsg
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
	 * Send a ZMQControl command and a given message to the specified receiver.
	 *
	 * @param zmqController - the zmq controller socket, IT IS NOT thread-safe
	 * @param msg - message to be appended to control command, can be null
	 */
	public static void sendZMQControlCommand(ZMQ.Socket zmqController, String receiverIdentity, ZMQControlCommand command, @Nullable ZMsg msg) {
		ZMsg toSend = ZMsg.newStringMsg(receiverIdentity, command.name());

		if (msg != null) {
			for (int i = 0; i <= msg.size(); i++) {
				toSend.add(msg.pop());
			}
		}
		toSend.send(zmqController);
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
	 * Polls the poller, gets a command and returns it together with the rest of the ZMsg.
	 * Note, that the ZMsg can either be null or empty, depending on the used ZMQControlCommand.
	 *
	 * @return the command or NONE if none exists or could not be parsed
	 */
	public static Pair<ZMQControlCommand, @Nullable ZMsg> getCommandAndMsg(ZMQ.Poller poller, int index) {
		if (poller.pollin(index)) {
			ZMsg zMsg = ZMsg.recvMsg(poller.getSocket(index));
			logger.debug("Received control message {}", zMsg);

			try {
				// drop message origin
				zMsg.pop();
				ZMQControlCommand command = ZMQControlCommand.valueOf(zMsg.pop().getString(ZMQ.CHARSET));
				return Pair.of(command, zMsg);

			} catch (IllegalArgumentException | NullPointerException e) {
				logger.warn("Received a ZMQControlCommand that cannot be parsed {}", zMsg);
			}
		}
		return Pair.of(ZMQControlCommand.NONE, null);
	}

}
