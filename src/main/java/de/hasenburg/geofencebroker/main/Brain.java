package de.hasenburg.geofencebroker.main;

import de.hasenburg.geofencebroker.communication.RouterCommunicator;
import de.hasenburg.geofencebroker.model.connections.ConnectionManager;
import de.hasenburg.geofencebroker.model.exceptions.CommunicatorException;
import de.hasenburg.geofencebroker.tasks.TaskManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMsg;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class Brain {

	private static final Logger logger = LogManager.getLogger();

	public static void main(String[] args) throws CommunicatorException, InterruptedException {

		RouterCommunicator router = new RouterCommunicator("tcp://localhost", 5559);
		router.init(null);
		BlockingQueue<ZMsg> blockingQueue = new LinkedBlockingDeque<>();
		router.startReceiving(blockingQueue);

		ConnectionManager connectionManager = new ConnectionManager();

		TaskManager taskManager = new TaskManager();
		// let's use two message processor tasks
		taskManager.runMessageProcessorTask(blockingQueue, router, connectionManager);
		taskManager.runMessageProcessorTask(blockingQueue, router, connectionManager);

		while (true) {
			logger.info(connectionManager.toString());
			Thread.sleep(2000);
		}

	}

}
