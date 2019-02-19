package de.hasenburg.geobroker.client.communication;

import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.communication.ZMQControlUtility;
import de.hasenburg.geobroker.commons.communication.ZMQProcess;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Optional;

/**
 * This client continuously receives messages from the connected server and writes them into a txt file.
 * The txt file is located at ./<identity>.txt
 *
 * When the client receives ORDERS.SEND, it sends the attached message to the server.
 */
public class ZMQProcess_StorageClient extends ZMQProcess {

	public enum ORDERS {
		SEND,
		CONFIRM,
		FAIL
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

	protected ZMQProcess_StorageClient(String address, int port, String identity)
			throws IOException {
		super(identity);
		this.address = address;
		this.port = port;

		CLIENT_ORDER_BACKEND = Utility.generateClientOrderBackendString(identity);
		writer = new BufferedWriter(new FileWriter(identity + ".txt"));
	}

	@Override
	public void run() {
		Thread.currentThread().setName(identity);

		ZMQ.Socket orders = context.createSocket(SocketType.REP);
		orders.bind(CLIENT_ORDER_BACKEND);

		ZMQ.Socket serverSocket = context.createSocket(SocketType.DEALER);
		serverSocket.setIdentity(identity.getBytes());
		serverSocket.connect(address + ":" + port);

		ZMQ.Poller poller = context.createPoller(3);
		int zmqControlIndex = ZMQControlUtility.connectWithPoller(context, poller, identity); // 0
		poller.register(serverSocket, ZMQ.Poller.POLLIN); // 1
		poller.register(orders, ZMQ.Poller.POLLIN); // 2

		while (!Thread.currentThread().isInterrupted()) {

			logger.trace("ZMQProcess_StorageClient waiting for orders");
			poller.poll(TIMEOUT_SECONDS * 1000);

			if (poller.pollin(zmqControlIndex)) {
				if (ZMQControlUtility.getCommand(poller, zmqControlIndex)
						.equals(ZMQControlUtility.ZMQControlCommand.KILL)) {
					break;
				}
			} else if (poller.pollin(1)) { // check if we received a message
				logger.trace("Received a message, writing it to file.");
				Optional<InternalClientMessage> serverMessage =
						InternalClientMessage.buildMessage(ZMsg.recvMsg(serverSocket, true));
				if (serverMessage.isPresent()) {
					try {
						writer.write(serverMessage.get().toString());
					} catch (IOException e) {
						logger.error("Could not write server message to file", e);
					}
				} else {
					logger.error("Server message was malformed or empty, so not written to file");
				}

			} else if (poller.pollin(2)) { // check if we got the order to send a message
				ZMsg order = ZMsg.recvMsg(orders);

				boolean valid = true;
				if (order.size() < 1) {
					logger.warn("Order has the wrong length {}" + order);
					valid = false;
				}
				String orderType = order.popString();

				if (valid && ORDERS.SEND.name().equals(orderType)) {
					logger.trace("Sending message to server");

					//the zMsg should consist of an InternalClientMessage only, as other entries are popped
					Optional<InternalClientMessage> clientMessageO = InternalClientMessage.buildMessage(order);

					if (clientMessageO.isPresent()) {
						clientMessageO.get().getZMsg().send(serverSocket);
						ZMsg.newStringMsg(ORDERS.CONFIRM.name()).send(orders);
					} else {
						logger.warn("Cannot run send as given message is incompatible");
						valid = false;
					}
				}
				if (!valid || !ORDERS.SEND.name().equals(orderType)) {
					// send order response if not already done
					ZMsg.newStringMsg(ORDERS.FAIL.name()).send(orders);
				}
			}
		} // end while loop

		// flush and close writer
		try {
			writer.flush();
			writer.close();
		} catch (IOException e) {
			logger.error("Could not flush and close writer", e);
		}

		// sub control socket
		context.destroySocket(poller.getSocket(0));

		// other sockets
		context.destroySocket(orders);
		context.destroySocket(serverSocket);
		logger.info("Shut down ZMQProcess_StorageClient, orders and server sockets were destroyed.");
	}

}
