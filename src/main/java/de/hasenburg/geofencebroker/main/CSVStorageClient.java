package de.hasenburg.geofencebroker.main;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.communication.ZMQProcessManager;
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

import java.io.IOException;
import java.util.Random;

public class CSVStorageClient {

	private static final Logger logger = LogManager.getLogger();

	private ZMQProcessManager processManager;
	private String identifier;
	ZMQ.Socket orderSocket;


	public CSVStorageClient(@Nullable String identifier, String address, int port, ZMQProcessManager processManager)
			throws IOException {
		if (identifier == null) {
			Random random = new Random();
			identifier = "CSVStorageClient-" + System.nanoTime();
		}

		this.identifier = identifier;
		this.processManager = processManager;
		processManager.runZMQProcess_CSVStorageClient(address, port, identifier);
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
		processManager.sendKillCommandToZMQProcess(getIdentity());
	}

	public ZMsg sendInternalClientMessage(InternalClientMessage message) {
		ZMsg orderMessage = ZMsg.newStringMsg(ZMQProcess_SimpleClient.ORDERS.SEND.name());
		ZMsg internalClientMessage = message.getZMsg();
		for (int i = 0; i <= internalClientMessage.size(); i++) {
			orderMessage.add(internalClientMessage.pop());
		}

		orderMessage.send(orderSocket);
		return(ZMsg.recvMsg(orderSocket));
	}

	public static void main (String[] args) throws IOException {
		ZMQProcessManager processManager = new ZMQProcessManager();
		CSVStorageClient client = new CSVStorageClient(null, "tcp://localhost", 5559, processManager);

		// connect
		InternalClientMessage clientMessage = new InternalClientMessage(ControlPacketType.CONNECT, new CONNECTPayload(
				Location.random()));
		client.sendInternalClientMessage(clientMessage);

		// wait 2 seconds, we should receive a CONNACK and write it to CSV.
		Utility.sleepNoLog(2000, 0);

		client.tearDownClient();
		if (processManager.tearDown(3000)) {
			logger.info("CSVStorageClient shut down properly.");
		} else {
			logger.fatal("ProcessManager reported that processes are still running: {}", processManager.getIncompleteZMQProcesses());
		}
		System.exit(0);
	}

}
