package de.hasenburg.geobroker.server.main;

import de.hasenburg.geobroker.commons.communication.ZMQProcessManager;
import de.hasenburg.geobroker.server.communication.ZMQProcessStarter;
import de.hasenburg.geobroker.server.distribution.BrokerAreaManager;
import de.hasenburg.geobroker.server.main.server.ServerLifecycle;
import de.hasenburg.geobroker.server.main.server.SingleGeoBrokerServerLogic;
import de.hasenburg.geobroker.server.matching.SingleGeoBrokerMatchingLogic;
import de.hasenburg.geobroker.server.storage.client.ClientDirectory;
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Server {

	private static final Logger logger = LogManager.getLogger();

	public static void main(String[] args) {

		Configuration configuration = Configuration.readDefaultConfiguration();

		// TODO should be configurable
		ServerLifecycle lifecycle = new ServerLifecycle(new SingleGeoBrokerServerLogic());

		logger.info("Starting lifecycle");
		lifecycle.run(configuration);
		logger.info("End of lifecycle reached, shutting down");

	}

}
