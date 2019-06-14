package de.hasenburg.geobroker.server.main;

import de.hasenburg.geobroker.server.main.server.DisGBSubscriberMatchingServerLogic;
import de.hasenburg.geobroker.server.main.server.IServerLogic;
import de.hasenburg.geobroker.server.main.server.ServerLifecycle;
import de.hasenburg.geobroker.server.main.server.SingleGeoBrokerServerLogic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Server {

	private static final Logger logger = LogManager.getLogger();

	public static void main(String[] args) {
		Configuration configuration;

		if (args.length > 0) {
			configuration = Configuration.readConfiguration(args[0]);
		} else {
			configuration = new Configuration(5, 2);
		}

		IServerLogic logic;
		if (Configuration.Mode.disgb_subscriberMatching.equals(configuration.getMode())) {
			logger.info("GeoBroker is configured to run geo-distributed (subscriber matching)");
			logic = new DisGBSubscriberMatchingServerLogic();
		} else if (Configuration.Mode.disgb_publisherMatching.equals(configuration.getMode())) {
			logger.info("GeoBroker is configured to run geo-distributed (publisher matching)");
			logic = new DisGBSubscriberMatchingServerLogic();
		} else {
			logger.info("GeoBroker is configured to run standalone");
			logic = new SingleGeoBrokerServerLogic();
		}

		ServerLifecycle lifecycle = new ServerLifecycle(logic);

		logger.info("Starting lifecycle of broker {}", configuration.getBrokerId());
		logger.info("Config: {}", configuration.toString());
		lifecycle.run(configuration);
		logger.info("End of lifecycle reached, shutting down");

	}

}
