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

		Configuration configuration = Configuration.readConfiguration("configuration.toml");
		IServerLogic logic;
		if (Configuration.Mode.single.equals(configuration.getMode())) {
			logger.info("GeoBroker is configured to run standalone");
			logic = new SingleGeoBrokerServerLogic();
		} else {
			logger.info("GeoBroker is configured to run geo-distributed");
			logic = new DisGBSubscriberMatchingServerLogic();
		}

		ServerLifecycle lifecycle = new ServerLifecycle(logic);

		logger.info("Starting lifecycle");
		lifecycle.run(configuration);
		logger.info("End of lifecycle reached, shutting down");

	}

}
