package de.hasenburg.geofencebroker.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LocationTest {

	private static final Logger logger = LogManager.getLogger();

	@Test
	public void testStringLocation() {
		logger.info("RUNNING testLocation TEST");
		Location location = Location.random();
		logger.debug(location.toString());
		assertEquals(location, Location.fromString(location.toString()).get());
		logger.info("FINISHED TEST");
	}

}
