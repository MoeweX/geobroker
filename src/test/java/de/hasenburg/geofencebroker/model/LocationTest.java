package de.hasenburg.geofencebroker.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

	@Test
	public void testDistance() {
		logger.info("RUNNING testDistance TEST");
		Location location = new Location(40.0, 40.0);
		Location location2 = new Location(35.0, 35.0);
		logger.info(Location.distanceInMeters(location, location2));
		assertTrue(Location.distanceInMeters(location, location2) < 710000);
		assertTrue(Location.distanceInMeters(location, location2) > 700000);

		logger.info("FINISHED TEST");
	}

}
