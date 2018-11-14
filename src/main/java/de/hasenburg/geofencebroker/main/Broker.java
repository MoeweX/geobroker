package de.hasenburg.geofencebroker.main;

import de.hasenburg.geofencebroker.communication.ZMQProcessManager;
import de.hasenburg.geofencebroker.model.connections.ClientDirectory;
import de.hasenburg.geofencebroker.model.storage.TopicAndGeofenceMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Broker {

	private static final Logger logger = LogManager.getLogger();

	public static void main(String[] args) throws InterruptedException {

		Configuration configuration = Configuration.readDefaultConfiguration();

		ClientDirectory clientDirectory = new ClientDirectory();
		TopicAndGeofenceMapper topicAndGeofenceMapper = new TopicAndGeofenceMapper(configuration);

		ZMQProcessManager processManager = new ZMQProcessManager();
		processManager.runZMQProcess_Broker("tcp://localhost", 5559, "broker");
		processManager.runZMQProcess_MessageProcessor("message_processor", clientDirectory, topicAndGeofenceMapper);

		while (true) {
			logger.info(clientDirectory.toString());
			Thread.sleep(2000);
		}

	}

}
