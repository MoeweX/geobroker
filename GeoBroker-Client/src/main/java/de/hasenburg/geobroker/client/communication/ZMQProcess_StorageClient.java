package de.hasenburg.geobroker.client.communication;

import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.communication.ZMQControlUtility;
import de.hasenburg.geobroker.commons.communication.ZMQProcess;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * This client continuously receives messages from the connected server and writes them into a txt file. The txt file is
 * located at ./<identity>.txt
 */
public class ZMQProcess_StorageClient extends ZMQProcess {

	public enum ORDERS {
		SEND, CONFIRM, FAIL
	}

	private static final Logger logger = LogManager.getLogger();
	private static final int TIMEOUT_SECONDS = 10; // logs when not received in time, but repeats

	// Client processing backend, accepts REQ and answers with REP
	private final String CLIENT_ORDER_BACKEND;

	// Address and port of the server the client connects to
	private String address;
	private int port;

	// Writer
	private BufferedWriter writer;

	// socket indices
	private final int ORDER_INDEX = 0;
	private final int SERVER_INDEX = 1;

	ZMQProcess_StorageClient(String address, int port, String identity) throws IOException {
		super(identity);
		this.address = address;
		this.port = port;

		CLIENT_ORDER_BACKEND = Utility.generateClientOrderBackendString(identity);
		writer = new BufferedWriter(new FileWriter(identity + ".txt"));
	}

	@Override
	protected List<Socket> bindAndConnectSockets(ZContext context) {
		Socket[] socketArray = new ZMQ.Socket[2];

		Socket orders = context.createSocket(SocketType.REP);
		orders.bind(CLIENT_ORDER_BACKEND);
		socketArray[ORDER_INDEX] = orders;

		Socket serverSocket = context.createSocket(SocketType.DEALER);
		serverSocket.setIdentity(identity.getBytes());
		serverSocket.connect(address + ":" + port);
		socketArray[SERVER_INDEX] = serverSocket;

		return Arrays.asList(socketArray);
	}

	@Override
	protected void processZMQControlCommandOtherThanKill(ZMQControlUtility.ZMQControlCommand zmqControlCommand) {
		// no other commands are of interest
	}

	@Override
	protected void processZMsg(int socketIndex, ZMsg msg) {
		switch (socketIndex) {
			case SERVER_INDEX: // got a reply from the server

				logger.trace("Received a message, writing it to file.");
				Optional<InternalClientMessage> serverMessage = InternalClientMessage.buildMessage(msg);
				if (serverMessage.isPresent()) {
					try {
						writer.write(serverMessage.get().toString());
					} catch (IOException e) {
						logger.error("Could not write server message to file", e);
					}
				} else {
					logger.error("Server message was malformed or empty, so not written to file");
				}

				break;
			case ORDER_INDEX: // got the order to do something

				boolean valid = true;
				if (msg.size() < 1) {
					logger.warn("Order has the wrong length {}", msg);
					valid = false;
				}
				String orderType = msg.popString();

				if (valid && ORDERS.SEND.name().equals(orderType)) {
					logger.trace("Sending message to server");

					//the zMsg should consist of an InternalClientMessage only, as other entries are popped
					Optional<InternalClientMessage> clientMessageO = InternalClientMessage.buildMessage(msg);

					if (clientMessageO.isPresent()) {
						clientMessageO.get().getZMsg().send(sockets.get(SERVER_INDEX));
						ZMsg.newStringMsg(ORDERS.CONFIRM.name()).send(sockets.get(ORDER_INDEX));
					} else {
						logger.warn("Cannot run send as given message is incompatible");
						valid = false;
					}
				}
				if (!valid || !ORDERS.SEND.name().equals(orderType)) {
					// send order response if not already done
					ZMsg.newStringMsg(ORDERS.FAIL.name()).send(sockets.get(ORDER_INDEX));
				}

				break;
			default:
				logger.error("Cannot process message for socket at index {}, as this index is not known.", socketIndex);
		}
	}

	@Override
	protected void shutdownCompleted() {
		// flush and close writer
		try {
			writer.flush();
			writer.close();
		} catch (IOException e) {
			logger.error("Could not flush and close writer", e);
		}

		logger.info("Shut down ZMQProcess_StorageClient {}", identity);
	}

}
