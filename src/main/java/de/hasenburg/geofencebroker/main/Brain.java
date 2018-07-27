package de.hasenburg.geofencebroker.main;

import de.hasenburg.geofencebroker.communication.Broker;
import de.hasenburg.geofencebroker.model.connections.ConnectionManager;
import de.hasenburg.geofencebroker.model.exceptions.CommunicatorException;
import de.hasenburg.geofencebroker.tasks.TaskManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Brain {

	private static final Logger logger = LogManager.getLogger();

	public static void main(String[] args) throws CommunicatorException, InterruptedException {

		Broker broker = new Broker("tcp://localhost", 5559);
		broker.init();

		ConnectionManager connectionManager = new ConnectionManager();

		TaskManager taskManager = new TaskManager();
		taskManager.runZMQMessageProcessorTask(broker.getContext(), connectionManager);

		while (true) {
			logger.info(connectionManager.toString());
			Thread.sleep(2000);
		}

	}

}
