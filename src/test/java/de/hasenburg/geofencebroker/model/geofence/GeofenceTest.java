package de.hasenburg.geofencebroker.model.geofence;

import de.hasenburg.geofencebroker.model.Location;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GeofenceTest {

	private static final Logger logger = LogManager.getLogger();

	@Test
	public void testInCircleInfinite() {
		logger.info("RUNNING testInCircleInfinite TEST");

		Geofence geofence = new Geofence(Location.random(), -1.0);
		assertTrue(geofence.locationInFence(Location.random()));

		logger.info("FINISHED TEST");
	}

	@Test
	public void testInCircle() {
		logger.info("RUNNING testInCircle TEST");

		Geofence geofence = new Geofence(Location.random(), 10.0);
		assertTrue(geofence.locationInFence(geofence.getCircleLocation()));

		logger.info("FINISHED TEST");
	}

	//@Test
	public void testCircleTolerance() {
		fail("Not yet implemented");
	}
}
