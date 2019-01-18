package de.hasenburg.geofencebroker.main;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.communication.ZMQProcessManager;
import de.hasenburg.geofencebroker.communication.ZMQProcess_BenchmarkClient;
import de.hasenburg.geofencebroker.communication.ZMQProcess_SimpleClient;
import de.hasenburg.geofencebroker.model.InternalClientMessage;
import de.hasenburg.geofencebroker.model.payload.CONNECTPayload;
import de.hasenburg.geofencebroker.model.spatial.Location;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import javax.rmi.CORBA.Util;
import java.io.IOException;
import java.util.Random;

public class BenchmarkClient {

	private static final Logger logger = LogManager.getLogger();

	private ZMQProcessManager processManager;
	private String identifier;
	ZMQ.Socket orderSocket;
	private boolean tearedDown = false;


	public BenchmarkClient(@Nullable String identifier, String address, int port, ZMQProcessManager processManager) {
		if (identifier == null) {
			Random random = new Random();
			identifier = "BenchmarkClient-" + System.nanoTime();
		}

		this.identifier = identifier;
		this.processManager = processManager;
		processManager.runZMQProcess_BenchmarkClient(address, port, identifier);
		orderSocket = processManager.getContext().createSocket(SocketType.REQ);
		orderSocket.setIdentity(identifier.getBytes());
		orderSocket.connect(Utility.generateClientOrderBackendString(identifier));

		logger.info("Created client {}", identifier);
	}

	public String getIdentity() {
		return identifier;
	}

	public void tearDownClient() {
		tearedDown = true;
		orderSocket.setLinger(0);
		orderSocket.close();
		processManager.sendKillCommandToZMQProcess(getIdentity());
	}

	public ZMsg sendInternalClientMessage(InternalClientMessage message) {
		ZMsg orderMessage = ZMsg.newStringMsg(ZMQProcess_BenchmarkClient.ORDERS.SEND.name());
		ZMsg internalClientMessage = message.getZMsg();
		for (int i = 0; i <= internalClientMessage.size(); i++) {
			orderMessage.add(internalClientMessage.pop());
		}

		if (!tearedDown) {
			orderMessage.send(orderSocket);
			return (ZMsg.recvMsg(orderSocket));
		}
		return null;
	}

	public static void main (String[] args) {
		ZMQProcessManager processManager = new ZMQProcessManager();
		BenchmarkClient client = new BenchmarkClient(null, "tcp://localhost", 5559, processManager);

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
