package de.hasenburg.geobroker.client.main;

import de.hasenburg.geobroker.client.communication.InternalClientMessage;
import de.hasenburg.geobroker.client.communication.ZMQProcessStarter;
import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.communication.ZMQControlUtility;
import de.hasenburg.geobroker.commons.communication.ZMQProcessManager;
import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.KryoSerializer;
import de.hasenburg.geobroker.commons.model.message.payloads.CONNECTPayload;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Random;

@SuppressWarnings("WeakerAccess")
public class StorageClient {

	private static final Logger logger = LogManager.getLogger();

	private KryoSerializer kryo = new KryoSerializer();
	private ZMQProcessManager processManager;
	private String identifier;

	public StorageClient(@Nullable String identifier, String address, int port, ZMQProcessManager processManager)
			throws IOException {
		if (identifier == null) {
			Random random = new Random();
			identifier = "StorageClient-" + System.nanoTime();
		}

		this.identifier = identifier;
		this.processManager = processManager;
		ZMQProcessStarter.runZMQProcess_StorageClient(processManager, address, port, identifier);

		logger.info("Created client {}", identifier);
	}

	public String getIdentity() {
		return identifier;
	}

	public void tearDownClient() {
		processManager.sendCommandToZMQProcess(getIdentity(), ZMQControlUtility.ZMQControlCommand.KILL);
	}

	public void sendInternalClientMessage(InternalClientMessage message) {
		processManager.sendCommandToZMQProcess(getIdentity(),
				ZMQControlUtility.ZMQControlCommand.SEND_ZMsg,
				message.getZMsg(kryo));
	}

	public static void main(String[] args) throws IOException {
		ZMQProcessManager processManager = new ZMQProcessManager();
		StorageClient client = new StorageClient(null, "localhost", 5559, processManager);

		// connect
		InternalClientMessage clientMessage = new InternalClientMessage(ControlPacketType.CONNECT,
				new CONNECTPayload(Location.random()));
		client.sendInternalClientMessage(clientMessage);

		// wait 2 seconds, we should receive a CONNACK and write it to file.
		Utility.sleepNoLog(2000, 0);

		client.tearDownClient();
		if (processManager.tearDown(3000)) {
			logger.info("StorageClient shut down properly.");
		} else {
			logger.fatal("ProcessManager reported that processes are still running: {}",
					processManager.getIncompleteZMQProcesses());
		}
		System.exit(0);
	}

}
