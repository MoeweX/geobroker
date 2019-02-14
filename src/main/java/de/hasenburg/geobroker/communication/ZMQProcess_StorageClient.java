package de.hasenburg.geobroker.communication;

import de.hasenburg.geobroker.main.Utility;
import de.hasenburg.geobroker.model.InternalClientMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Optional;

/**
 * This client continuously receives messages from the connected broker and writes them into a txt file.
 * The txt file is located at ./<identity>.txt
 *
 * When the client receives ORDERS.SEND, it sends the attached message to the broker.
 */
public class ZMQProcess_StorageClient implements Runnable {

	public enum ORDERS {
		SEND,
		CONFIRM,
		FAIL
	}

	private static final Logger logger = LogManager.getLogger();
	private static final int TIMEOUT_SECONDS = 10; // logs when not received in time, but repeats

	// Client processing backend, accepts REQ and answers with REP
	private final String CLIENT_ORDER_BACKEND;

	// Address and port of the broker the client connects to
	private String address;
	private int port;
	private String identity;

	// Socket and context
	private ZContext context;

	// Writer
	private BufferedWriter writer;

	protected ZMQProcess_StorageClient(String address, int port, String identity, ZContext context)
			throws IOException {
		this.address = address;
		this.port = port;
		this.identity = identity;

		CLIENT_ORDER_BACKEND = Utility.generateClientOrderBackendString(identity);
		writer = new BufferedWriter(new FileWriter(identity + ".txt"));

		this.context = context;
	}

	@Override
	public void run() {
		Thread.currentThread().setName(identity);

		ZMQ.Socket orders = context.createSocket(SocketType.REP);
		orders.bind(CLIENT_ORDER_BACKEND);

		ZMQ.Socket brokerSocket = context.createSocket(SocketType.DEALER);
		brokerSocket.setIdentity(identity.getBytes());
		brokerSocket.connect(address + ":" + port);

		ZMQ.Poller poller = context.createPoller(3);
		int zmqControlIndex = ZMQControlUtility.connectWithPoller(context, poller, identity); // 0
		poller.register(brokerSocket, ZMQ.Poller.POLLIN); // 1
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
				Optional<InternalClientMessage> brokerMessage =
						InternalClientMessage.buildMessage(ZMsg.recvMsg(brokerSocket, true));
				if (brokerMessage.isPresent()) {
					try {
						writer.write(brokerMessage.get().toString());
					} catch (IOException e) {
						logger.error("Could not write broker message to file", e);
					}
				} else {
					logger.error("Broker message was malformed or empty, so not written to file");
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
					logger.trace("Sending message to broker");

					//the zMsg should consist of an InternalClientMessage only, as other entries are popped
					Optional<InternalClientMessage> clientMessageO = InternalClientMessage.buildMessage(order);

					if (clientMessageO.isPresent()) {
						clientMessageO.get().getZMsg().send(brokerSocket);
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

		// other sockets (might be optional, kill nevertheless)
		context.destroySocket(orders);
		context.destroySocket(brokerSocket);
		logger.info("Shut down ZMQProcess_StorageClient, orders and broker sockets were destroyed.");
	}

}
