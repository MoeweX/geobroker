package de.hasenburg.geofencebroker.communication;

import de.hasenburg.geofencebroker.main.Utility;
import de.hasenburg.geofencebroker.model.InternalClientMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * This client's purpose is to do benchmarking. For that, it measures the latency between a send message, and the
 * appropriate answer from the broker. When the client receives ORDERS.SEND, it sends the attached message to the
 * broker. For the following message type combinations, latencies are measured:
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
 */
public class ZMQProcess_BenchmarkClient implements Runnable {

	public enum ORDERS {
		SEND, CONFIRM, FAIL
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

	// Benchmarking variables
	private Map<ControlPacketType, List<Long>> timestamps;
	private int receivedPublishMessages = 0;

	protected ZMQProcess_BenchmarkClient(String address, int port, String identity, ZContext context) {
		this.address = address;
		this.port = port;
		this.identity = identity;

		CLIENT_ORDER_BACKEND = Utility.generateClientOrderBackendString(identity);

		timestamps = new HashMap<>();
		timestamps.put(ControlPacketType.CONNECT, new ArrayList<>());
		timestamps.put(ControlPacketType.CONNACK, new ArrayList<>());
		timestamps.put(ControlPacketType.DISCONNECT, new ArrayList<>());
		timestamps.put(ControlPacketType.PINGREQ, new ArrayList<>());
		timestamps.put(ControlPacketType.PINGRESP, new ArrayList<>());
		timestamps.put(ControlPacketType.SUBSCRIBE, new ArrayList<>());
		timestamps.put(ControlPacketType.SUBACK, new ArrayList<>());
		timestamps.put(ControlPacketType.PUBLISH, new ArrayList<>());
		timestamps.put(ControlPacketType.PUBACK, new ArrayList<>());

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
				Optional<InternalClientMessage> brokerMessage =
						InternalClientMessage.buildMessage(ZMsg.recvMsg(brokerSocket, true));
				logger.trace("Received message {}, storing timestamp", brokerMessage);
				if (brokerMessage.isPresent()) {
					if (ControlPacketType.PUBLISH.equals(brokerMessage.get().getControlPacketType())) {
						receivedPublishMessages++;
					} else {
						List<Long> longs = timestamps.get(brokerMessage.get().getControlPacketType());
						// check whether interesting control packet type -> put into correct list
						if (longs != null) {
							longs.add(timestamp);
						}
					}
				} else {
					logger.error("Broker message was malformed or empty, so no timestamp was stored");
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
						List<Long> longs = timestamps.get(clientMessageO.get().getControlPacketType());
						// check whether interesting control packet type -> put into correct list
						if (longs != null) {
							longs.add(timestamp);
						}
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

		// write results to disk
		writeResultsToDisk();

		// sub control socket
		context.destroySocket(poller.getSocket(0));

		// other sockets (might be optional, kill nevertheless)
		context.destroySocket(orders);
		context.destroySocket(brokerSocket);
		logger.info("Shut down ZMQProcess_BenchmarkClient, orders and broker sockets were destroyed.");
	}

	private void writeResultsToDisk() {
		String targetDir = "benchmark_client_results/";
		String targetFile = targetDir + identity + ".txt";

		try {
			File f = new File(targetDir);
			f.mkdirs();
			BufferedWriter writer = new BufferedWriter(new FileWriter(targetFile));
			writer.write("numberOfReceivedPublishMessages," + receivedPublishMessages + "\n");
			for (ControlPacketType controlPacketType : timestamps.keySet()) {
				writer.write(controlPacketType.name() + ",");
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
