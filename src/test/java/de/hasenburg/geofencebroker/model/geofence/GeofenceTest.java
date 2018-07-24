package de.hasenburg.geofencebroker.model.geofence;

import de.hasenburg.geofencebroker.model.JSONable;
import de.hasenburg.geofencebroker.model.Location;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GeofenceTest {

	private static final Logger logger = LogManager.getLogger();

	@Test
	public void testEmpty() {
		logger.info("RUNNING testEmpty TEST");

		Geofence geofence = new Geofence();
		String json = geofence.toJSON();
		logger.debug(json);
		assertEquals(geofence, Geofence.fromJSON(json));

		logger.info("FINISHED TEST");
	}

	@Test
	public void testCircle() {
		logger.info("RUNNING testCircle TEST");

		Geofence geofence = new Geofence();
		geofence.buildCircle(Location.random(), 10.0);
		String json = geofence.toJSON();
		logger.debug(json);
		assertEquals(geofence, Geofence.fromJSON(json));

		logger.info("FINISHED TEST");
	}

	//@Test
	public void testCircleTolerance() {
		fail("Not yet implemented");
	}
}
