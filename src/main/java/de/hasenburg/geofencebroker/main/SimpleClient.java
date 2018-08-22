package de.hasenburg.geofencebroker.main;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.communication.DealerCommunicator;
import de.hasenburg.geofencebroker.communication.ZMQControlUtility;
import de.hasenburg.geofencebroker.communication.ZMQProcessManager;
import de.hasenburg.geofencebroker.model.InternalClientMessage;
import de.hasenburg.geofencebroker.model.exceptions.CommunicatorException;
import de.hasenburg.geofencebroker.model.payload.CONNECTPayload;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class SimpleClient {

	public enum ORDERS {
		SEND,
		RECEIVE,
		CONFIRM,
		FAIL
	}

	private static final Logger logger = LogManager.getLogger();

	private ZMQProcessManager processManager;
	private String identifier;
	ZMQ.Socket orderSocket;


	public SimpleClient(@Nullable String identifier, String address, int port, ZMQProcessManager processManager) {
		if (identifier == null) {
			Random random = new Random();
			identifier = "SimpleClient-" + System.nanoTime();
		}

		this.identifier = identifier;
		this.processManager = processManager;
		processManager.runZMQProcess_SimpleClient(address, port, identifier);
		orderSocket = processManager.getContext().createSocket(ZMQ.REQ);
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
		ZMsg orderMessage = ZMsg.newStringMsg(ORDERS.SEND.name());
		ZMsg internalClientMessage = message.getZMsg();
		for (int i = 0; i <= internalClientMessage.size(); i++) {
			orderMessage.add(internalClientMessage.pop());
		}

		orderMessage.send(orderSocket);
		return(ZMsg.recvMsg(orderSocket));
	}

	public InternalClientMessage receiveInternalClientMessage() {
		ZMsg orderMessage = ZMsg.newStringMsg(ORDERS.RECEIVE.name());

		// send order
		orderMessage.send(orderSocket);
		final Optional<InternalClientMessage> clientMessageO =
				InternalClientMessage.buildMessage(ZMsg.recvMsg(orderSocket));

		return clientMessageO.orElse(null);
	}

	public static void main (String[] args) {
	    ZMQProcessManager processManager = new ZMQProcessManager();
	    SimpleClient client = new SimpleClient(null, "tcp://localhost", 5559, processManager);

	    // connect
		InternalClientMessage clientMessage = new InternalClientMessage(ControlPacketType.CONNECT, new CONNECTPayload());
		client.sendInternalClientMessage(clientMessage);

		// receive one message
		InternalClientMessage response = client.receiveInternalClientMessage();
		logger.info("Received broker answer: {}", response);

		client.tearDownClient();
		if (processManager.tearDown(3000)) {
			logger.info("SimpleClient shut down properly.");
		} else {
			logger.fatal("ProcessManager reported that processes are still running: {}", processManager.getIncompleteZMQProcesses());
		}
		System.exit(0);
	}



}
