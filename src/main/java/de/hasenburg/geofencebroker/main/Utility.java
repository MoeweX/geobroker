package de.hasenburg.geofencebroker.main;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Utility {

	private static final Logger logger = LogManager.getLogger();

	public static void sleep(long millis, int nanos) {
		try {
			Thread.sleep(millis, nanos);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.error("Interrupted my sleep :S -> interrupting!", e);
		}
	}

	public static void sleepNoLog(long millis, int nanos) {
		try {
			Thread.sleep(millis, nanos);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

}
