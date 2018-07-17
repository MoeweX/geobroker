package de.hasenburg.geofencebroker.main;

import de.hasenburg.geofencebroker.communication.RouterCommunicator;
import de.hasenburg.geofencebroker.exceptions.CommunicatorException;
import de.hasenburg.geofencebroker.tasks.TaskManager;
import org.zeromq.ZMsg;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class Brain {

	public static void main(String[] args) throws CommunicatorException {

		RouterCommunicator router = new RouterCommunicator("tcp://localhost", 5559);
		router.init(null);
		BlockingQueue<ZMsg> blockingQueue = new LinkedBlockingDeque<>();
		router.startReceiving(blockingQueue);

		TaskManager taskManager = new TaskManager();
		taskManager.runMessageProcessorTask(blockingQueue, router);

	}

}
