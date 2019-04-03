package de.hasenburg.geobroker.server.main;

import de.hasenburg.geobroker.commons.communication.ZMQProcessManager;
import de.hasenburg.geobroker.server.communication.ZMQProcessStarter;
import de.hasenburg.geobroker.server.distribution.BrokerAreaManager;
import de.hasenburg.geobroker.server.storage.client.ClientDirectory;
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Server {

	private static final Logger logger = LogManager.getLogger();

	public static void main(String[] args) throws InterruptedException {

		Configuration configuration = Configuration.readDefaultConfiguration();

		ClientDirectory clientDirectory = new ClientDirectory();
		TopicAndGeofenceMapper topicAndGeofenceMapper = new TopicAndGeofenceMapper(configuration);
		BrokerAreaManager brokerAreaManager = new BrokerAreaManager(configuration.getBrokerId());
		brokerAreaManager.setup_DefaultFile();

		ZMQProcessManager processManager = new ZMQProcessManager();
		ZMQProcessStarter.runZMQProcess_Server(processManager, "tcp://localhost", configuration.getPort(), "broker");
		for (int i = 1; i <= configuration.getMessageProcessors(); i++) {
			ZMQProcessStarter.runZMQProcess_MessageProcessor(processManager,
															 "message_processor-" + i,
															 clientDirectory,
															 topicAndGeofenceMapper,
															 brokerAreaManager);

		}

		while (true) {
			logger.info(clientDirectory.toString());
			Thread.sleep(2000);
		}

	}

}
