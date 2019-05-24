package de.hasenburg.geobroker.client.main;

import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.communication.ZMQControlUtility;
import de.hasenburg.geobroker.commons.communication.ZMQProcessManager;
import de.hasenburg.geobroker.commons.model.KryoSerializer;
import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.client.communication.ZMQProcessStarter;
import de.hasenburg.geobroker.client.communication.ZMQProcess_BenchmarkClient;
import de.hasenburg.geobroker.client.communication.InternalClientMessage;
import de.hasenburg.geobroker.commons.model.message.payloads.CONNECTPayload;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Random;

public class BenchmarkClient {

	private static final Logger logger = LogManager.getLogger();

	private ZMQProcessManager processManager;
	private String identifier;
	ZMQ.Socket orderSocket;
	private KryoSerializer kryo = new KryoSerializer();


	public BenchmarkClient(@Nullable String identifier, String address, int port, ZMQProcessManager processManager) {
		if (identifier == null) {
			Random random = new Random();
			identifier = "BenchmarkClient-" + System.nanoTime();
		}

		this.identifier = identifier;
		this.processManager = processManager;
		ZMQProcessStarter.runZMQProcess_BenchmarkClient(processManager, address, port, identifier);
		orderSocket = processManager.getContext().createSocket(SocketType.REQ);
		orderSocket.setIdentity(identifier.getBytes());
		orderSocket.connect(Utility.generateClientOrderBackendString(identifier));

		logger.info("Created client {}", identifier);
	}

	public String getIdentity() {
		return identifier;
	}

	public void tearDownClient() {
		orderSocket.setLinger(0);
		orderSocket.close();
		processManager.sendCommandToZMQProcess(getIdentity(), ZMQControlUtility.ZMQControlCommand.KILL);
	}

	public ZMsg sendInternalClientMessage(InternalClientMessage message) {
		ZMsg orderMessage = ZMsg.newStringMsg(ZMQProcess_BenchmarkClient.ORDERS.SEND.name());
		ZMsg internalClientMessage = message.getZMsg(kryo);
		for (int i = 0; i <= internalClientMessage.size(); i++) {
			orderMessage.add(internalClientMessage.pop());
		}

		orderMessage.send(orderSocket);
		return (ZMsg.recvMsg(orderSocket));
	}

	public static void main (String[] args) {
		ZMQProcessManager processManager = new ZMQProcessManager();
		BenchmarkClient client = new BenchmarkClient(null, "localhost", 5559, processManager);

		// connect
		InternalClientMessage clientMessage = new InternalClientMessage(ControlPacketType.CONNECT, new CONNECTPayload(
				Location.random()));
		client.sendInternalClientMessage(clientMessage);

		// wait 2 seconds, we should receive a CONNACK and wrote timestamps to file.
		Utility.sleepNoLog(2000, 0);

		client.tearDownClient();
		if (processManager.tearDown(3000)) {
			logger.info("BenchmarkClient shut down properly.");
		} else {
			logger.fatal("ProcessManager reported that processes are still running: {}", processManager.getIncompleteZMQProcesses());
		}
		System.exit(0);
	}

}
