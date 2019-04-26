package de.hasenburg.geolife;

import de.hasenburg.geobroker.server.main.Configuration;
import de.hasenburg.geobroker.server.main.server.ServerLifecycle;
import de.hasenburg.geobroker.server.main.server.SingleGeoBrokerServerLogic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GeolifeServer {

	private static final Logger logger = LogManager.getLogger();

	public static void main(String[] args) {

		String configurationName = args[0];

		Configuration configuration = Configuration.readConfigurationFromS3(configurationName);

		ServerLifecycle lifecycle = new ServerLifecycle(new SingleGeoBrokerServerLogic());

		logger.info("Starting lifecycle");
		lifecycle.run(configuration);
		logger.info("End of lifecycle reached, shutting down");

	}

}
