package de.hasenburg.geofencebroker.main.moving_client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public class ClientManager {

	private static final Logger logger = LogManager.getLogger();

	private Date startupTime;
	private String configurationName;
	private String managerName;

	public ClientManager(String managerName, Date startupTime, String configurationName) {
		this.managerName = managerName;
		this.startupTime = startupTime;
		this.configurationName = configurationName;
	}

	public void start() {
		// create configuration
		Configuration c = Configuration.readConfigurationFromS3(configurationName, managerName);

		// download required files from S3
		// TODO

		// calculate how much time to wait for startup
		Date now = new Date();
		//in milliseconds
		long diff = (startupTime.getTime() - now.getTime());

		if (diff < 0) {
			logger.fatal("Startup time seems to be {} seconds in the past", diff / -1000);
			System.exit(1);
		}
		try {
			logger.info("Waiting {} seconds for startup", diff / 1000);
			Thread.sleep(diff);
		} catch (InterruptedException e) {
			logger.fatal("Interrupted while waiting for startup", e);
			System.exit(1);
		}

		logger.info("Starting up!");
	}

	private List<Route> downloadRequiredFiles(int index, int count) {
		List<Route> routes = new ArrayList<>();
		for (int i = index; i <= count; i++) {
			Route r = null;
			// TODO create route with the help of geolifedatasethelper
			routes.add(r);
		}
		logger.info("Successfully downloaded {} routes", routes.size());
		return routes;
	}

	/**
	 *
	 * Requires two args:
	 * args[0] = startup time in format yyyy-MM-dd_HH:mm
	 * args[1] = configuration name
	 *
	 * @param args - the args
	 */
	public static void main (String[] args) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH:mm");
		Date startupTime = null;
		try {
			startupTime = format.parse(args[0]);
		} catch (ParseException e) {
			logger.fatal("Invalid startup time");
			System.exit(1);
		}
		String configurationName = args[1];

		// there should be a file in ./name whose name is the managerName
		File f = new File("./name");
		String managerName = Objects.requireNonNull(f.listFiles())[0].getName();

		logger.info("Starting up {} at {} based on configuration {}", managerName, format.format(startupTime), configurationName);
		ClientManager clientManager = new ClientManager(managerName, startupTime, configurationName);
		clientManager.start();
	}

}
