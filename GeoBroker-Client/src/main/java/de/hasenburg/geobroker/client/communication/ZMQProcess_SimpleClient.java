package de.hasenburg.geobroker.client.communication;

import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.communication.ZMQControlUtility;
import de.hasenburg.geobroker.commons.communication.ZMQProcess;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Optional;

public class ZMQProcess_SimpleClient extends ZMQProcess {

	public enum ORDERS {
		SEND,
		RECEIVE,
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

	protected ZMQProcess_SimpleClient(String address, int port, String identity) {
		this.address = address;
		this.port = port;
		this.identity = identity;

		CLIENT_ORDER_BACKEND = Utility.generateClientOrderBackendString(identity);
	}

	@Override
	public void run() {
		Thread.currentThread().setName(identity);

		ZMQ.Socket orders = context.createSocket(SocketType.REP);
		orders.bind(CLIENT_ORDER_BACKEND);

		ZMQ.Socket brokerSocket = context.createSocket(SocketType.DEALER);
		brokerSocket.setIdentity(identity.getBytes());
		brokerSocket.connect(address + ":" + port);

		ZMQ.Poller poller = context.createPoller(1);
		int zmqControlIndex = ZMQControlUtility.connectWithPoller(context, poller, identity);
		poller.register(orders, ZMQ.Poller.POLLIN);

		while (!Thread.currentThread().isInterrupted()) {

			logger.trace("ZMQProcess_SimpleClient waiting for orders");
			poller.poll(TIMEOUT_SECONDS * 1000);

			if (poller.pollin(zmqControlIndex)) {
				if (ZMQControlUtility.getCommand(poller, zmqControlIndex)
						.equals(ZMQControlUtility.ZMQControlCommand.KILL)) {
					break;
				}
			} else if (poller.pollin(1)) { // check if we got an order
				ZMsg order = ZMsg.recvMsg(orders);

				boolean valid = true;
				if (order.size() < 1) {
					logger.warn("Order has the wrong length {}" + order);
					valid = false;
				}
				String orderType = order.popString();

				if (valid && ORDERS.RECEIVE.name().equals(orderType)) {
					logger.trace("Receiving message from broker");
					Optional<InternalClientMessage> brokerMessage = InternalClientMessage.buildMessage(ZMsg.recvMsg(brokerSocket, true));
					if (brokerMessage.isPresent()) {
						brokerMessage.get().getZMsg().send(orders);
					} else {
						logger.warn("Broker message malformed or empty");
						valid = false;
					}
				} else if (valid && ORDERS.SEND.name().equals(orderType)) {
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

				if (!valid) {
					// send order response if not already done
					ZMsg.newStringMsg(ORDERS.FAIL.name()).send(orders);
				}
			}

		} // end while loop

		// sub control socket
		context.destroySocket(poller.getSocket(0));

		// other sockets (might be optional, kill nevertheless)
		context.destroySocket(orders);
		context.destroySocket(brokerSocket);
		logger.info("Shut down ZMQProcess_SimpleClient, orders and broker sockets were destroyed.");
	}

}
