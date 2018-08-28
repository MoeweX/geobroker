package de.hasenburg.geofencebroker.model.spatial;

import de.hasenburg.geofencebroker.model.JSONable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class LocationTest {

	private static final Logger logger = LogManager.getLogger();

	private Location location;
	private final int N = 10000;

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
	public void toAndFromString() {
		String s = location.toString();
		logger.info("Location: {}", s);
		assertEquals(location, Location.fromString(s).orElse(null));
	}

	@Test
	public void toAndFromJson() {
		String json = JSONable.toJSON(location);
		logger.info("Location JSON: {}", json);
		assertEquals(location, JSONable.fromJSON(json, Location.class).orElse(null));
	}

	@Test
	public void toAndFromStringN() {
		long time = System.currentTimeMillis();
		for (int i = 0; i < N; i++) {
			assertEquals(location, Location.fromString(location.toString()).orElse(null));
		}
		logger.info("Created and read {} Strings in {}ms", N, System.currentTimeMillis() - time);
	}

	@Test
	public void toAndFromJsonN() {
		long time = System.currentTimeMillis();
		for (int i = 0; i < N; i++) {
			assertEquals(location, JSONable.fromJSON(JSONable.toJSON(location), Location.class).orElse(null));
		}
		logger.info("Created and read {} JSONs in {}ms", N, System.currentTimeMillis() - time);
	}
}