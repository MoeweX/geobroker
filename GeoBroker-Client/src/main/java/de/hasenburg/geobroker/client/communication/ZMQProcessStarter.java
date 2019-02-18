package de.hasenburg.geobroker.client.communication;

import de.hasenburg.geobroker.commons.communication.ZMQProcessManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class ZMQProcessStarter {

	private static final Logger logger = LogManager.getLogger();

	public static void runZMQProcess_SimpleClient(ZMQProcessManager processManager, String address, int port, String identity) {
		processManager.submitZMQProcess(identity, new ZMQProcess_SimpleClient(address, port, identity));
	}

	public static void runZMQProcess_StorageClient(ZMQProcessManager processManager, String address, int port, String identity) throws IOException {
		processManager.submitZMQProcess(identity, new ZMQProcess_StorageClient(address, port, identity));
	}

	public static void runZMQProcess_BenchmarkClient(ZMQProcessManager processManager, String address, int port, String identity) {
		processManager.submitZMQProcess(identity, new ZMQProcess_BenchmarkClient(address, port, identity));
	}
}
