package de.hasenburg.geobroker.commons.model.spatial;

import de.hasenburg.geobroker.commons.model.KryoSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class LocationTest {

	private static final Logger logger = LogManager.getLogger();

	private Location location;
	private final int N = 100000;

	@Before
	public void setUp() {
		location = Location.random();
		assertNotNull(location);
	}

	@Test
	public void equalsAndRandom() {
		assertEquals(location, location);
		assertNotEquals(location, Location.random());
	}

	@Test
	public void toAndFromByte() {
		KryoSerializer kryo = new KryoSerializer();
		// point set
		byte[] bytes = kryo.write(location);
		assertEquals(location, kryo.read(bytes, Location.class));

		// undefined
		bytes = kryo.write(Location.undefined());
		assertEquals(Location.undefined(), kryo.read(bytes, Location.class));
	}

	@Test
	public void toAndFromByteN() {
		KryoSerializer kryo = new KryoSerializer();
		long time = System.nanoTime();
		for (int i = 0; i < N; i++) {
			assertEquals(location, kryo.read(kryo.write(location), Location.class));
		}
		logger.info("Created and read {} locations in {}ms", N, (System.nanoTime() - time) / 1000000);
	}

	@Test
	public void testLocationInDistance() {
		Location berlin = new Location(52.5200, 13.4050);

		Location l450m = Location.locationInDistance(berlin, 0.450, 45);
		logger.info(berlin.distanceKmTo(l450m));
		assertEquals(0.450, berlin.distanceKmTo(l450m), 0.01);

		Location l0 = Location.locationInDistance(berlin, 223.4, 0);
		assertEquals(223.4, berlin.distanceKmTo(l0), 0.01);

		Location l360 = Location.locationInDistance(berlin, 223.4, 360);
		assertEquals(223.4, berlin.distanceKmTo(l360), 0.01);

		Location l361 = Location.locationInDistance(berlin, 1000, 361);
		assertEquals(1000, berlin.distanceKmTo(l361), 0.01);

		Location ln180 = Location.locationInDistance(berlin, 140.1, -180);
		assertEquals(140.1, berlin.distanceKmTo(ln180), 0.01);
	}

	@Test
	public void testDistance() {
		Location location = new Location(40.0, 40.0);
		Location location2 = new Location(35.0, 35.0);
		logger.info(location.distanceKmTo(location2));
		assertTrue(location.distanceKmTo(location2) < 710.000);
		assertTrue(location.distanceKmTo(location2) > 700.000);
	}

	@Test
	public void testDistanceBerlinHamburg() {
		Location berlin = new Location(52.5200, 13.4050);
		Location hamburg = new Location(53.511, 9.9937);
		logger.info(berlin.distanceKmTo(hamburg));
		assertEquals(253.375, berlin.distanceKmTo(hamburg), 0.5);
	}

	@Test
	public void testRandomInGeofence() {
		Geofence geofence = Geofence.circle(new Location(30, 30), 3.0);
		Location l = Location.randomInGeofence(geofence);
		assertTrue(geofence.contains(l));
	}
}