package de.hasenburg.geobroker.client.communication;

import com.esotericsoftware.kryo.Kryo;
import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.communication.ZMQControlUtility;
import de.hasenburg.geobroker.commons.communication.ZMQProcess;
import de.hasenburg.geobroker.commons.model.KryoSerializer;
import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.message.payloads.PUBLISHPayload;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
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
 * TODO: the benchmark client also does not has to use the order architecture, could be build similarly to StorageClient
 */
public class ZMQProcess_BenchmarkClient extends ZMQProcess {

	public enum ORDERS {
		SEND, CONFIRM, FAIL
	}

	private static final Logger logger = LogManager.getLogger();

	// Client processing backend, accepts REQ and answers with REP
	private final String CLIENT_ORDER_BACKEND;
	public KryoSerializer kryo = new KryoSerializer();

	// Address and port of the server the client connects to
	private String address;
	private int port;

	// Benchmarking variables
	private Map<Integer, List<Long>> timestamps;
	private int receivedForeignPublishMessages = 0;

	// socket indices
	private final int ORDER_INDEX = 0;
	private final int SERVER_INDEX = 1;

	ZMQProcess_BenchmarkClient(String address, int port, String identity) {
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
	protected void processZMQControlCommandOtherThanKill(ZMQControlUtility.ZMQControlCommand zmqControlCommand, ZMsg msg) {

	}

	@Override
	protected void processZMsg(int socketIndex, ZMsg msg) {
		long timestamp = new Date().getTime();

		switch (socketIndex) {
			case SERVER_INDEX: // got a reply from the server

				Optional<InternalClientMessage> serverMessage = InternalClientMessage.buildMessage(msg, kryo);
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
					Optional<InternalClientMessage> clientMessageO = InternalClientMessage.buildMessage(msg, kryo);

					if (clientMessageO.isPresent()) {
						List<Long> longs = timestamps.get(clientMessageO.get().getControlPacketType().ordinal());
						// check whether interesting control packet type -> put into correct list
						if (longs != null) {
							longs.add(timestamp);
						}
						clientMessageO.get().getZMsg(kryo).send(sockets.get(SERVER_INDEX));
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
		// write results to disk
		writeResultsToDisk();

		logger.info("Shut down ZMQProcess_BenchmarkClient {}", identity);
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
