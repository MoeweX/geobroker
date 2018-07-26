package de.hasenburg.geofencebroker.model.geofence;

import de.hasenburg.geofencebroker.model.Location;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.*;

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

	@Test
	public void testEquals() {
		logger.info("RUNNING testEquals TEST");

		Location l = Location.random();
		Geofence geofence1 = new Geofence(l, 10.0);
		Geofence geofence2 = new Geofence(l, 10.0);
		Geofence geofence3 = new Geofence(l, 10.1); // should not be equals
		assertEquals(geofence1, geofence2);
		assertNotEquals(geofence1, geofence3);

		logger.info("FINISHED TEST");
	}

	@Test
	public void testCircleTolerance() {
		logger.info("RUNNING testCircleTolerance TEST");


		Location l1 = new Location(30.0, 30.0);
		Location l2 = new Location(l1.getLatitude(), l1.getLongitude() + 0.00010); // 9.6 meters difference
		Location l3 = new Location(l1.getLatitude(), l1.getLongitude() + 0.00011); // 10.6 meters difference
		logger.debug(Location.distanceInMeters(l1, l2));
		logger.debug(Location.distanceInMeters(l1, l3));

		Geofence geofence1 = new Geofence(l1, 100.0);
		Geofence geofence2 = new Geofence(l2, 100.0);
		Geofence geofence3 = new Geofence(l3, 100.0);

		assertEquals(geofence1, geofence2);
		assertNotEquals(geofence1, geofence3);

		logger.info("FINISHED TEST");
	}


}
