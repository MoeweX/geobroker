package de.hasenburg.geobroker.client.communication;

import com.esotericsoftware.kryo.Kryo;
import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.communication.ZMQControlUtility;
import de.hasenburg.geobroker.commons.communication.ZMQProcess;
import de.hasenburg.geobroker.commons.model.KryoSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ZMQProcess_SimpleClient extends ZMQProcess {

	public enum ORDERS {
		SEND, RECEIVE, RECEIVE_WITH_TIMEOUT, CONFIRM, FAIL, EMPTY
	}

	private static final Logger logger = LogManager.getLogger();

	// Client processing backend, accepts REQ and answers with REP
	private final String CLIENT_ORDER_BACKEND;

	// Address and port of the server the client connects to
	private String address;
	private int port;
	public KryoSerializer kryo = new KryoSerializer();

	// socket indices
	private final int ORDER_INDEX = 0;
	private final int SERVER_INDEX = 1;

	// received message buffer
	List<InternalClientMessage> receivedMessages = new ArrayList<>();

	protected ZMQProcess_SimpleClient(String address, int port, String identity) {
		super(identity);
		this.address = address;
		this.port = port;

		CLIENT_ORDER_BACKEND = Utility.generateClientOrderBackendString(identity);
	}

	@Override
	protected List<ZMQ.Socket> bindAndConnectSockets(ZContext context) {
		Socket[] socketArray = new ZMQ.Socket[2];

		Socket orders = context.createSocket(SocketType.REP);
		orders.bind(CLIENT_ORDER_BACKEND);
		socketArray[ORDER_INDEX] = orders;

		Socket serverSocket = context.createSocket(SocketType.DEALER);
		serverSocket.setIdentity(identity.getBytes());
		serverSocket.connect("tcp://" + address + ":" + port);
		socketArray[SERVER_INDEX] = serverSocket;

		return Arrays.asList(socketArray);
	}

	@Override
	protected void processZMQControlCommandOtherThanKill(ZMQControlUtility.ZMQControlCommand zmqControlCommand,
														 ZMsg msg) {
		// no other commands are of interest
	}

	@Override
	protected void processZMsg(int socketIndex, ZMsg msg) {

		switch (socketIndex) {
			case SERVER_INDEX: // got a reply from the server

				logger.trace("Received message from server.");
				Optional<InternalClientMessage> serverMessage = InternalClientMessage.buildMessage(msg, kryo);
				if (serverMessage.isPresent()) {
					receivedMessages.add(serverMessage.get());
				} else {
					logger.warn("Server message malformed or empty");
				}

				break;
			case ORDER_INDEX: // got the order to do something

				boolean valid = true;
				if (msg.size() < 1) {
					logger.warn("Order has the wrong length {}", msg);
					valid = false;
				}
				String orderType = msg.popString();

				if (valid && ORDERS.RECEIVE.name().equals(orderType)) {
					logger.trace("ORDER = Receive from Broker");

					if (receivedMessages.size() > 0) {
						InternalClientMessage message = receivedMessages.remove(0);
						message.getZMsg(kryo).send(sockets.get(ORDER_INDEX));
					} else {
						// nothing received yet, so let's wait
						Optional<InternalClientMessage> messageO = InternalClientMessage.buildMessage(ZMsg.recvMsg(
								sockets.get(SERVER_INDEX),
								true), kryo);
						if (messageO.isPresent()) {
							messageO.get().getZMsg(kryo).send(sockets.get(ORDER_INDEX));
						} else {
							logger.warn("Server message malformed or empty");
							valid = false;
						}
					}
				} else if (valid && ORDERS.RECEIVE_WITH_TIMEOUT.name().equals(orderType)) {
					int timeout = 0;
					try {
						timeout = Integer.parseInt(msg.popString());
					} catch (NumberFormatException | NullPointerException e) {
						logger.warn("Receive with timeout did not contain a proper timeout, setting to 0ms");
					}

					logger.trace("ORDER = Receive from Broker (with timeout {})", timeout);

					// first check the buffer
					if (receivedMessages.size() > 0) {
						InternalClientMessage message = receivedMessages.remove(0);
						message.getZMsg(kryo).send(sockets.get(ORDER_INDEX));
						return;
					} else {
						// nothing received yet, so let's wait
						poller.poll(timeout);

						if (poller.pollin(SERVER_INDEX)) {
							Optional<InternalClientMessage> messageO = InternalClientMessage.buildMessage(ZMsg.recvMsg(
									sockets.get(SERVER_INDEX)), kryo);
							messageO.ifPresent(internalClientMessage -> internalClientMessage.getZMsg(kryo)
																							 .send(sockets.get(
																									 ORDER_INDEX)));
							return;
						} else {
							logger.debug(
									"Did not receive a server response in time, or another order needs to be executed");
						}
					}

					// send back an empty response
					ZMsg.newStringMsg(ORDERS.EMPTY.name()).send(sockets.get(ORDER_INDEX));
					return; // no need to do final valid check as we have already replied
				} else if (valid && ORDERS.SEND.name().equals(orderType)) {
					logger.trace("ORDER = Send to Broker");

					//the zMsg should consist of an InternalClientMessage only, as other entries are popped
					Optional<InternalClientMessage> clientMessageO = InternalClientMessage.buildMessage(msg, kryo);

					if (clientMessageO.isPresent()) {
						clientMessageO.get().getZMsg(kryo).send(sockets.get(SERVER_INDEX));
						ZMsg.newStringMsg(ORDERS.CONFIRM.name()).send(sockets.get(ORDER_INDEX));
					} else {
						logger.warn("Cannot run send as given message is incompatible");
						valid = false;
					}
				}

				if (!valid) {
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
		logger.info("Shut down ZMQProcess_SimpleClient {}", identity);
	}

}
