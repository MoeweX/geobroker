package de.hasenburg.geolife;

import de.hasenburg.geobroker.commons.communication.ZMQProcessManager;
import de.hasenburg.geobroker.server.communication.ZMQProcessStarter;
import de.hasenburg.geobroker.server.distribution.BrokerAreaManager;
import de.hasenburg.geobroker.server.main.Configuration;
import de.hasenburg.geobroker.server.storage.client.ClientDirectory;
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GeolifeServer {

	private static final Logger logger = LogManager.getLogger();

	public static void main(String[] args) throws InterruptedException {

		String configurationName = args[0];

		Configuration configuration = Configuration.readConfigurationFromS3(configurationName);

		ClientDirectory clientDirectory = new ClientDirectory();
		TopicAndGeofenceMapper topicAndGeofenceMapper = new TopicAndGeofenceMapper(configuration);
		BrokerAreaManager brokerAreaManager = new BrokerAreaManager("broker");
		brokerAreaManager.setup_DefaultFile();

		ZMQProcessManager processManager = new ZMQProcessManager();
		ZMQProcessStarter.runZMQProcess_Server(processManager, "tcp://0.0.0.0", 5559, "broker");
		for (int i = 1; i <= configuration.getMessageProcessors(); i++) {
			ZMQProcessStarter.runZMQProcess_MessageProcessor(processManager,
															 "message_processor-" + i,
															 clientDirectory,
															 topicAndGeofenceMapper,
															 brokerAreaManager);
		}

		while (true) {
			logger.info("Currently, there are {} connected clients", clientDirectory.getNumberOfClients());
			Thread.sleep(10000);
		}

	}

}
