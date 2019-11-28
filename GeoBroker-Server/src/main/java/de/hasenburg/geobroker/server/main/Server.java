package de.hasenburg.geobroker.server.main;

import de.hasenburg.geobroker.server.main.server.*;
import de.hasenburg.geobroker.server.main.server.other.SingleNoGeoServerLogic;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class Server {

	private static final Logger logger = LogManager.getLogger();
	private static HTTPServer prometheusServer;

	public static void main(String[] args) {
		Configuration configuration;

		if (args.length > 0) {
			configuration = Configuration.readConfiguration(args[0]);
		} else {
			configuration = new Configuration(5, 2);
		}

		// TODO only start the server / default exports if specified in config file
		//Prometheus Init
		try {
			prometheusServer = new HTTPServer(1234);
		} catch (IOException e) {
			logger.warn("Prometheus can't open an http server on port 1234", e);
		}
		// Expose stats about jvm
		DefaultExports.initialize();

		IServerLogic logic;
		if (Configuration.Mode.disgb_subscriberMatching.equals(configuration.getMode())) {
			logger.info("GeoBroker is configured to run geo-distributed (subscriber matching)");
			logic = new DisGBSubscriberMatchingServerLogic();
		} else if (Configuration.Mode.disgb_publisherMatching.equals(configuration.getMode())) {
			logger.info("GeoBroker is configured to run geo-distributed (publisher matching)");
			logic = new DisGBPublisherMatchingServerLogic();
		} else if (Configuration.Mode.single.equals(configuration.getMode())){
			logger.info("GeoBroker is configured to run standalone");
			logic = new SingleGeoBrokerServerLogic();
		} else {
			logger.info("GeoBroker is configured to run in \"other\" mode");
			if (Configuration.OtherMode.single_noGeo.equals(configuration.getOtherMode())) {
				logic = new SingleNoGeoServerLogic();
			} else {
				logger.fatal("No valid other mode provided");
				System.exit(1);
				logic = null; // stupid compiler forces me to write this
			}
		}

		ServerLifecycle lifecycle = new ServerLifecycle(logic);

		logger.info("Starting lifecycle of broker {}", configuration.getBrokerId());
		logger.info("Config: {}", configuration.toString());
		lifecycle.run(configuration);
		logger.info("End of lifecycle reached, shutting down");

		if (prometheusServer != null){
			prometheusServer.stop();
		}
	}
}
