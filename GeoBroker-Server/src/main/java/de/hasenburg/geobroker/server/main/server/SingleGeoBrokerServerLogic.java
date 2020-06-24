package de.hasenburg.geobroker.server.main.server;

import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.communication.ZMQProcessManager;
import de.hasenburg.geobroker.server.communication.ZMQProcessStarter;
import de.hasenburg.geobroker.server.main.Configuration;
import de.hasenburg.geobroker.server.matching.SingleGeoBrokerMatchingLogic;
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper;
import de.hasenburg.geobroker.server.storage.client.ClientDirectory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

public class SingleGeoBrokerServerLogic implements IServerLogic {

	private static final Logger logger = LogManager.getLogger();

	private Configuration configuration;
	private SingleGeoBrokerMatchingLogic matchingLogic;
	private ZMQProcessManager processManager;
	private ClientDirectory clientDirectory;

	@Override
	public void loadConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	@Override
	public void initializeFields() {
		clientDirectory = new ClientDirectory();
		TopicAndGeofenceMapper topicAndGeofenceMapper = new TopicAndGeofenceMapper(configuration);

		matchingLogic = new SingleGeoBrokerMatchingLogic(clientDirectory, topicAndGeofenceMapper);
		processManager = new ZMQProcessManager();
	}

	@Override
	public void startServer() {
		ZMQProcessStarter.runZMQProcess_Server(processManager,
				"0.0.0.0",
				configuration.getPort(),
				configuration.getBrokerId());
		for (int number = 1; number <= configuration.getMessageProcessors(); number++) {
			ZMQProcessStarter.runZMQProcess_MessageProcessor(processManager,
					configuration.getBrokerId(),
					number,
					matchingLogic);
		}
		logger.info("Started server successfully!");
	}

	@Override
	public void serverIsRunning() {
		AtomicBoolean keepRunning = new AtomicBoolean(true);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> keepRunning.set(false)));

		while (keepRunning.get()) {
			logger.info(clientDirectory.toString());
			Utility.sleepNoLog(200000, 0);
		}
	}

	@Override
	public void cleanUp() {
		processManager.tearDown(2000);
		logger.info("Tear down completed");
	}

	/*****************************************************************
	 * Generated methods
	 ****************************************************************/

	public ClientDirectory getClientDirectory() {
		return clientDirectory;
	}
}
