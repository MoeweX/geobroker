package de.hasenburg.geofencebroker.model.spatial;

import de.hasenburg.geofencebroker.model.JSONable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotEquals;

@SuppressWarnings("ConstantConditions")
public class GeofenceTest {

	private static final Logger logger = LogManager.getLogger();

	@Test
	public void testEquals() {
		Geofence fence = getRectAroundBerlin();
		Geofence fence2 = getRectAroundBerlin();
		Geofence fence3 = getTriangleAroundBerlin();
		assertEquals(fence, fence2);
		logger.info("Geofences {} and {} equal", fence, fence2);
		assertNotEquals(fence, fence3);
		logger.info("Geofences {} and {} do not equal", fence, fence3);
	}

	@Test
	public void toAndFromJson() {
		Geofence fence = getRectAroundBerlin();
		String json = JSONable.toJSON(fence);
		logger.info("JSON 1: {}", json);
		Geofence fence2 = JSONable.fromJSON(json, Geofence.class).get();
		logger.info("JSON 2: {}", JSONable.toJSON(fence2));
		assertEquals(fence, fence2);
		logger.info("Geofences {} and {} still equal after JSON stuff", fence, fence2);
	}

	private static Geofence getRectAroundBerlin() {
		return Geofence.polygon(List.of(
				new Location(53.0, 14.0),
				new Location(53.0, 13.0),
				new Location(52.0, 13.0),
				new Location(52.0, 14.0)));
	}

	private static Geofence getTriangleAroundBerlin() {
		return Geofence.polygon(List.of(
				new Location(54, 12.0),
				new Location(52, 15.0),
				new Location(50, 12.0)));
	}

}
