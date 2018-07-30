package de.hasenburg.geofencebroker.main;

import de.hasenburg.geofencebroker.communication.ZMQProcessManager;
import de.hasenburg.geofencebroker.model.connections.ConnectionManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Brain {

	private static final Logger logger = LogManager.getLogger();

	public static void main(String[] args) throws InterruptedException {

		ConnectionManager connectionManager = new ConnectionManager();

		ZMQProcessManager processManager = new ZMQProcessManager();
		processManager.runZMQProcess_Broker("tcp://localhost", 5559, "broker");
		processManager.runZMQProcess_MessageProcessor("message_processor", connectionManager);

		while (true) {
			logger.info(connectionManager.toString());
			Thread.sleep(2000);
		}

	}

}
