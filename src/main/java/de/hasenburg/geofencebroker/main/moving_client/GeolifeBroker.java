package de.hasenburg.geofencebroker.main.moving_client;

import de.hasenburg.geofencebroker.communication.ZMQProcessManager;
import de.hasenburg.geofencebroker.main.Configuration;
import de.hasenburg.geofencebroker.model.clients.ClientDirectory;
import de.hasenburg.geofencebroker.model.storage.TopicAndGeofenceMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GeolifeBroker {

	private static final Logger logger = LogManager.getLogger();

	public static void main(String[] args) throws InterruptedException {

		String configurationName = args[0];

		Configuration configuration = Configuration.readConfigurationFromS3(configurationName);

		ClientDirectory clientDirectory = new ClientDirectory();
		TopicAndGeofenceMapper topicAndGeofenceMapper = new TopicAndGeofenceMapper(configuration);

		ZMQProcessManager processManager = new ZMQProcessManager();
		processManager.runZMQProcess_Broker("tcp://0.0.0.0", 5559, "broker");
		for (int i = 1; i <= configuration.getMessageProcessors(); i++) {
			processManager.runZMQProcess_MessageProcessor("message_processor-" + i, clientDirectory, topicAndGeofenceMapper);
		}

		while (true) {
			logger.info("Currently, there are {} connected clients", clientDirectory.getNumberOfClients());
			Thread.sleep(10000);
		}

	}

}
