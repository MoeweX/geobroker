package de.hasenburg.geofencebroker.communication;

import de.hasenburg.geofencebroker.main.Utility;
import de.hasenburg.geofencebroker.model.clients.ClientDirectory;
import de.hasenburg.geofencebroker.model.storage.TopicAndGeofenceMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class ZMQProcessManager {

	private static final Logger logger = LogManager.getLogger();

	private final ExecutorService pool = Executors.newCachedThreadPool();
	private final ConcurrentHashMap<String, Future<?>> zmqProcesses = new ConcurrentHashMap<>();

	private final ZContext context;
	private final ZMQ.Socket zmqController;

	public ZMQProcessManager() {
		context = new ZContext(1);
		zmqController = ZMQControlUtility.createZMQControlSocket(context);
		logger.info("Started ZMQProcessManager");
	}

	public ZContext getContext() {
		return context;
	}

	/**
	 * Tries to tear down ZMQProcessManager. If futures do not complete in the given time, returns false and does not
	 * kill the context.
	 *
	 * @param timeout - timeout in microseconds
	 * @return true if teared down in timeout time
	 */
	public boolean tearDown(int timeout) {
		// send kill command to all
		List<String> toKill = getIncompleteZMQProcesses();
		toKill.forEach(identity -> sendKillCommandToZMQProcess(identity));

		int tries = 0;
		while (tries < timeout) {
			if (getIncompleteZMQProcesses().isEmpty()) {
				break;
			}
			Utility.sleepNoLog(1, 0);
			tries++;
		}

		// if anything still lives, return false
		if (!getIncompleteZMQProcesses().isEmpty()) {
			return false;
		}

		// now we can close the zmq controller
		context.destroySocket(zmqController);

		if (!context.getSockets().isEmpty()) {
			logger.warn("There are still open sockets in ZContext: {}. " +
								"They are being closed now, but is it intended that they are still open?",
						context.getSockets());
		}

		context.destroy();
		logger.info("Teared down ZMQProcessManager");
		return true;
	}

	public void sendKillCommandToZMQProcess(String identity) {
		logger.debug("Sending kill command to {}", identity);
		ZMQControlUtility.sendZMQControlCommand(zmqController, identity, ZMQControlUtility.ZMQControlCommand.KILL);
	}

	public List<String> getIncompleteZMQProcesses() {
		List<String> list = new ArrayList<>();
		for (Map.Entry<String, Future<?>> entry : zmqProcesses.entrySet()) {
			if (!entry.getValue().isDone()) {
				list.add(entry.getKey());
			} else {
				// if not active anymore, remove
				logger.debug("ZMQProcess {} has completed", entry.getKey());
				zmqProcesses.remove(entry.getKey());
			}
		}
		logger.trace("These processes are incomplete: " + list);
		return list;
	}

	/*****************************************************************
	 * Process Starter
	 ****************************************************************/

	public void runZMQProcess_MessageProcessor(String identity, ClientDirectory clientDirectory,
											   TopicAndGeofenceMapper topicAndGeofenceMapper) {
		if (getIncompleteZMQProcesses().contains(identity)) {
			logger.error("Cannot start ZMQProcess with identity {}, as one with this identity already exists",
						 identity);
			return;
		}
		Future<?> process = pool.submit(new ZMQProcess_MessageProcessor(identity,
																		context,
																		clientDirectory,
																		topicAndGeofenceMapper));
		zmqProcesses.put(identity, process);
		logger.info("Started {} with identity {}", ZMQProcess_MessageProcessor.class.getSimpleName(), identity);
	}

	public void runZMQProcess_Broker(String address, int port, String identity) {
		if (getIncompleteZMQProcesses().contains(identity)) {
			logger.error("Cannot start ZMQProcess with identity {}, as one with this identity already exists",
						 identity);
			return;
		}
		Future<?> process = pool.submit(new ZMQProcess_Broker(address, port, identity, context));
		zmqProcesses.put(identity, process);
		logger.info("Started {} with identity {}", ZMQProcess_Broker.class.getSimpleName(), identity);
	}

	public void runZMQProcess_SimpleClient(String address, int port, String identity) {
		if (getIncompleteZMQProcesses().contains(identity)) {
			logger.error("Cannot start ZMQProcess with identity {}, as one with this identity already exists",
						 identity);
			return;
		}
		Future<?> process = pool.submit(new ZMQProcess_SimpleClient(address, port, identity, context));
		zmqProcesses.put(identity, process);
		logger.info("Started {} with identity {}", ZMQProcess_SimpleClient.class.getSimpleName(), identity);
	}

	public void runZMQProcess_CSVStorageClient(String address, int port, String identity) throws IOException {
		if (getIncompleteZMQProcesses().contains(identity)) {
			logger.error("Cannot start ZMQProcess with identity {}, as one with this identity already exists",
						 identity);
			return;
		}
		Future<?> process = pool.submit(new ZMQProcess_CSVStorageClient(address, port, identity, context));
		zmqProcesses.put(identity, process);
		logger.info("Started {} with identity {}", ZMQProcess_CSVStorageClient.class.getSimpleName(), identity);
	}

}
