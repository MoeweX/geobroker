package de.hasenburg.geofencebroker.model.storage;

import de.hasenburg.geofencebroker.model.spatial.Location;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.locationtech.spatial4j.exception.InvalidShapeException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;

public class RasterTest {

	private static final Logger logger = LogManager.getLogger();

	@Test(expected = InvalidShapeException.class)
	public void testPrivateCalculateIndexGranularity1()
			throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

		Raster raster = new Raster(1);

		// make calculateIndexLocation testable
		Method calculateIndexLocation = raster.getClass().getDeclaredMethod("calculateIndexLocation", Location.class);
		calculateIndexLocation.setAccessible(true);

		Location calculatedIndex;

		// even
		calculatedIndex = (Location) calculateIndexLocation.invoke(raster, new Location(10, -10));
		assertEquals(new Location(10, -10), calculatedIndex);

		// many fractions
		calculatedIndex = (Location) calculateIndexLocation.invoke(raster, new Location(10.198, -11.198));
		assertEquals(new Location(10, -12), calculatedIndex);

		// exact boundary
		calculatedIndex = (Location) calculateIndexLocation.invoke(raster, new Location(90, -180));
		assertEquals(new Location(90, -180), calculatedIndex);

		// out of bounds, expect throw
		calculateIndexLocation.invoke(raster, new Location(91, -181));
	}

	@Test(expected = InvalidShapeException.class)
	public void testPrivateCalculateIndexGranularity10()
			throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

		Raster raster = new Raster(10);

		// make calculateIndexLocation testable
		Method calculateIndexLocation = raster.getClass().getDeclaredMethod("calculateIndexLocation", Location.class);
		calculateIndexLocation.setAccessible(true);

		Location calculatedIndex;

		// even
		calculatedIndex = (Location) calculateIndexLocation.invoke(raster, new Location(10, -10));
		assertEquals(new Location(10, -10), calculatedIndex);

		// many fractions
		calculatedIndex = (Location) calculateIndexLocation.invoke(raster, new Location(10.198, -11.198));
		assertEquals(new Location(10.1, -11.2), calculatedIndex);

		// exact boundary
		calculatedIndex = (Location) calculateIndexLocation.invoke(raster, new Location(90, -180));
		assertEquals(new Location(90, -180), calculatedIndex);

		// out of bounds, expect throw
		calculateIndexLocation.invoke(raster, new Location(91, -181));
	}

	@Test(expected = InvalidShapeException.class)
	public void testPrivateCalculateIndexGranularity100()
			throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

		Raster raster = new Raster(100);

		// make calculateIndexLocation testable
		Method calculateIndexLocation = raster.getClass().getDeclaredMethod("calculateIndexLocation", Location.class);
		calculateIndexLocation.setAccessible(true);

		Location calculatedIndex;

		// even
		calculatedIndex = (Location) calculateIndexLocation.invoke(raster, new Location(10, -10));
		assertEquals(new Location(10, -10), calculatedIndex);

		// many fractions
		calculatedIndex = (Location) calculateIndexLocation.invoke(raster, new Location(10.198, -11.198));
		assertEquals(new Location(10.19, -11.2), calculatedIndex);

		// exact boundary
		calculatedIndex = (Location) calculateIndexLocation.invoke(raster, new Location(90, -180));
		assertEquals(new Location(90, -180), calculatedIndex);

		// out of bounds, expect throw
		calculateIndexLocation.invoke(raster, new Location(91, -181));
	}

}