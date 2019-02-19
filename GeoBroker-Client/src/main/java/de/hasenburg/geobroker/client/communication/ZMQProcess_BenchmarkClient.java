package de.hasenburg.geobroker.client.communication;

import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.communication.ZMQControlUtility;
import de.hasenburg.geobroker.commons.communication.ZMQProcess;
import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.message.payloads.PUBLISHPayload;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * This client's purpose is to do benchmarking. For that, it measures the latency between a send message, and the
 * appropriate answer from the server. When the client receives ORDERS.SEND, it sends the attached message to the
 * server. For the following message type combinations, latencies are measured:
 *
 * - CONNECT -> CONNACK or DISCONNECT
 *
 * - PINGREQ -> PINGRESP
 *
 * - SUBSCRIBE -> SUBACK
 *
 * - PUBLISH -> PUBACK
 *
 * Furthermore, the client counts all PUBLISH messages it receives.
 *
 * WARNING: This client waits 5 seconds after it received the kill command so that its sockets have time to transmit
 * remaining messages. During that interval, it will not parse any incoming messages.
 */
public class ZMQProcess_BenchmarkClient extends ZMQProcess {

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

	// Benchmarking variables
	private Map<Integer, List<Long>> timestamps;
	private int receivedForeignPublishMessages = 0;

	protected ZMQProcess_BenchmarkClient(String address, int port, String identity) {
		super(identity);
		this.address = address;
		this.port = port;

		CLIENT_ORDER_BACKEND = Utility.generateClientOrderBackendString(identity);

		timestamps = new HashMap<>();
		timestamps.put(ControlPacketType.CONNECT.ordinal(), new ArrayList<>());
		timestamps.put(ControlPacketType.CONNACK.ordinal(), new ArrayList<>());
		timestamps.put(ControlPacketType.DISCONNECT.ordinal(), new ArrayList<>());
		timestamps.put(ControlPacketType.PINGREQ.ordinal(), new ArrayList<>());
		timestamps.put(ControlPacketType.PINGRESP.ordinal(), new ArrayList<>());
		timestamps.put(ControlPacketType.SUBSCRIBE.ordinal(), new ArrayList<>());
		timestamps.put(ControlPacketType.SUBACK.ordinal(), new ArrayList<>());
		timestamps.put(ControlPacketType.PUBLISH.ordinal(), new ArrayList<>());
		timestamps.put(999, new ArrayList<>()); // own received publish messages
		timestamps.put(ControlPacketType.PUBACK.ordinal(), new ArrayList<>());
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

			logger.trace("ZMQProcess_BenchmarkClient waiting for orders");
			poller.poll(TIMEOUT_SECONDS * 1000);
			long timestamp = new Date().getTime();

			if (poller.pollin(zmqControlIndex)) {
				if (ZMQControlUtility
						.getCommand(poller, zmqControlIndex)
						.equals(ZMQControlUtility.ZMQControlCommand.KILL)) {
					break;
				}
			} else if (poller.pollin(1)) { // check if we received a message
				Optional<InternalClientMessage> serverMessage =
						InternalClientMessage.buildMessage(ZMsg.recvMsg(serverSocket, true));
				logger.trace("Received message {}, storing timestamp", serverMessage);
				if (serverMessage.isPresent()) {
					if (ControlPacketType.PUBLISH.equals(serverMessage.get().getControlPacketType())) {
						@SuppressWarnings("OptionalGetWithoutIsPresent") PUBLISHPayload payload =
								serverMessage.get().getPayload().getPUBLISHPayload().get();
						if (payload.getContent().startsWith(identity + "+")) {
							// this is our own publish message that was received from the server
							timestamps.get(999).add(timestamp);
						} else {
							// this is a foreign publish messages
							receivedForeignPublishMessages++;
						}
					} else {
						List<Long> longs = timestamps.get(serverMessage.get().getControlPacketType().ordinal());
						// check whether interesting control packet type -> put into correct list
						if (longs != null) {
							longs.add(timestamp);
						}
					}
				} else {
					logger.error("Server message was malformed or empty, so no timestamp was stored");
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
						List<Long> longs = timestamps.get(clientMessageO.get().getControlPacketType().ordinal());
						// check whether interesting control packet type -> put into correct list
						if (longs != null) {
							longs.add(timestamp);
						}
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

		// write results to disk
		writeResultsToDisk();

		// let's wait 5 seconds so that all messages are send properly
		Utility.sleepNoLog(5000, 0);

		// sub control socket
		context.destroySocket(poller.getSocket(0));

		// other sockets
		context.destroySocket(orders);
		context.destroySocket(serverSocket);
		logger.info("Shut down ZMQProcess_BenchmarkClient, orders and server sockets were destroyed.");
	}

	private void writeResultsToDisk() {
		String targetDir = "latest_benchmark_client_results/";
		String targetFile = targetDir + identity + ".txt";

		try {
			File f = new File(targetDir);
			f.mkdirs();
			BufferedWriter writer = new BufferedWriter(new FileWriter(targetFile));
			writer.write("receivedForeignPublishMessages," + receivedForeignPublishMessages + "\n");
			for (Integer controlPacketType : timestamps.keySet()) {
				String name = "OWN-PUBLISH";
				if (controlPacketType != 999) {
					name = ControlPacketType.values()[controlPacketType].name();
				}
				writer.write(name + ",");
				for (Long aLong : timestamps.get(controlPacketType)) {
					writer.write(aLong + ",");
				}
				writer.write("\n");
			}
			writer.flush();
			writer.close();
			logger.info("Benchmark results have been written to {}", targetFile);
		} catch (IOException e) {
			logger.error("Could not write benchmarking results to disk!", e);
		}
	}

}
