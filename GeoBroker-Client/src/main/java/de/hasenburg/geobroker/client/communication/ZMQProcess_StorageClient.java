package de.hasenburg.geobroker.client.communication;

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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * This client continuously receives messages from the connected server and writes them into a txt file. The txt file is
 * located at ./<identity>.txt
 */
public class ZMQProcess_StorageClient extends ZMQProcess {

	private static final Logger logger = LogManager.getLogger();

	// Address and port of the server the client connects to
	private String address;
	private int port;
	public KryoSerializer kryo = new KryoSerializer();

	// Writer
	private BufferedWriter writer;

	// socket indices
	private final int SERVER_INDEX = 0;

	ZMQProcess_StorageClient(String address, int port, String identity) throws IOException {
		super(identity);
		this.address = address;
		this.port = port;

		writer = new BufferedWriter(new FileWriter(identity + ".txt"));
	}

	@Override
	protected List<Socket> bindAndConnectSockets(ZContext context) {
		Socket[] socketArray = new ZMQ.Socket[1];

		Socket serverSocket = context.createSocket(SocketType.DEALER);
		serverSocket.setIdentity(identity.getBytes());
		serverSocket.connect("tcp://" + address + ":" + port);
		socketArray[SERVER_INDEX] = serverSocket;

		return Arrays.asList(socketArray);
	}

	@Override
	protected void processZMQControlCommandOtherThanKill(ZMQControlUtility.ZMQControlCommand zmqControlCommand, ZMsg msg) {
		if (ZMQControlUtility.ZMQControlCommand.SEND_ZMsg.equals(zmqControlCommand)) {
			logger.trace("Sending a message to the server.");

			//the zMsg should consist of an InternalClientMessage only
			Optional<InternalClientMessage> clientMessageO = InternalClientMessage.buildMessage(msg, kryo);

			if (clientMessageO.isPresent()) {
				clientMessageO.get().getZMsg(kryo).send(sockets.get(SERVER_INDEX));
			} else {
				logger.warn("Cannot send message to server as not an InternalClientMessage" + msg);
			}
		}
	}

	@Override
	protected void processZMsg(int socketIndex, ZMsg msg) {
		if (socketIndex != SERVER_INDEX) { // got a reply from the server
			logger.error("Cannot process message for socket at index {}, as this index is not known.", socketIndex);
		}

		logger.trace("Received a message, writing it to file.");
		Optional<InternalClientMessage> serverMessage = InternalClientMessage.buildMessage(msg, kryo);
		if (serverMessage.isPresent()) {
			try {
				writer.write(serverMessage.get().toString());
			} catch (IOException e) {
				logger.error("Could not write server message to file", e);
			}
		} else {
			logger.error("Server message was malformed or empty, so not written to file");
		}
	}

	@Override
	protected void shutdownCompleted() {
		// flush and close writer
		try {
			writer.flush();
			writer.close();
		} catch (IOException e) {
			logger.error("Could not flush and close writer", e);
		}

		logger.info("Shut down ZMQProcess_StorageClient {}", identity);
	}

}
