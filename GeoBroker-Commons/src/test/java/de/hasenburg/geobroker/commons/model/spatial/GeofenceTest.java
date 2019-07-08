package de.hasenburg.geobroker.commons.model.spatial;

import de.hasenburg.geobroker.commons.model.KryoSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.*;

@SuppressWarnings("ConstantConditions")
public class GeofenceTest {

	private static final Logger logger = LogManager.getLogger();
	private static KryoSerializer kryo = new KryoSerializer();

	@Test
	public void toAndFromByte() {
		Geofence fence = berlinRectangle();
		byte[] bytes = kryo.write(fence);
		Geofence fence2 = kryo.read(bytes, Geofence.class);
		assertEquals(fence, fence2);
		logger.info("Geofences {} and {} still equal after Byte stuff", fence, fence2);
	}

	@Test
	public void toAndFromByteUndefined() {
		Geofence fence1 = Geofence.undefined();
		byte[] bytes = kryo.write(fence1);
		Geofence fence2 = kryo.read(bytes, Geofence.class);
		assertEquals(fence1, fence2);
		logger.info("Geofences {} and {} still equal after Byte stuff", fence1, fence2);
	}

	@Test
	public void toAndFromByteCircle() {
		Geofence fence = Geofence.circle(Location.random(), 1.4);
		byte[] bytes = kryo.write(fence);
		Geofence fence2 = kryo.read(bytes, Geofence.class);
		assertEquals(fence, fence2);
		logger.info("Geofences {} and {} still equal after Byte stuff", fence, fence2);
	}

	@Test
	public void toAndFromByteCircle2() {
		Geofence fence = Geofence.circle(new Location(40.007499, 116.320013), 0.1);
		byte[] bytes = kryo.write(fence);
		Geofence fence2 = kryo.read(bytes, Geofence.class);
		assertEquals(fence, fence2);
		logger.info("Geofences {} and {} still equal after Byte stuff", fence, fence2);
	}

	@Test
	public void testEquals() {
		Geofence fence = berlinRectangle();
		Geofence fence2 = berlinRectangle();
		Geofence fence3 = berlinTriangle();
		Geofence fence4 = Geofence.undefined();
		assertEquals(fence, fence2);
		logger.info("Geofences {} and {} equal", fence, fence2);
		assertNotEquals(fence, fence3);
		logger.info("Geofences {} and {} do not equal", fence, fence3);
		assertEquals(fence4, Geofence.undefined());
	}

	@Test
	public void testContains() {
		Location berlin = new Location(52.52, 13.405);
		Location hamburg = new Location(53.511, 9.9937);
		assertTrue(berlinRectangle().contains(berlin));
		assertFalse(berlinRectangle().contains(hamburg));
		assertFalse(Geofence.undefined().contains(berlin));
		logger.info("Geofence contains Berlin but not Hamburg");
	}

	@Test
	public void testContainsCircle() {
		Location l = Location.random();
		Geofence fence = Geofence.circle(l, 1.9);
		assertTrue(fence.contains(l));
	}

	@Test
	public void testDisjoint() {
		assertTrue(berlinRectangle().disjoint(datelineRectangle()));
		assertFalse(berlinRectangle().disjoint(berlinTriangle()));
		assertFalse(berlinRectangle().disjoint(Geofence.undefined()));
		logger.info("Disjoint calculation works properly");
	}

	@Test
	public void testRect() {
		assertTrue(berlinRectangle().isRectangle());
		assertFalse(berlinTriangle().isRectangle());
		logger.info("Able to detect rectangles");
	}

	@Test
	public void testBoundingBoxBerlin() {
		Geofence geofence = berlinRectangle();
		assertEquals(new Location(53, 13), geofence.getBoundingBoxNorthWest());
		assertEquals(new Location(53, 14), geofence.getBoundingBoxNorthEast());
		assertEquals(new Location(52, 14), geofence.getBoundingBoxSouthEast());
		assertEquals(new Location(52, 13), geofence.getBoundingBoxSouthWest());
		logger.info("Bounding box calculations work properly for Berlin");
	}

	@Test
	public void testBoundingBoxDateline() {
		Geofence geofence = datelineRectangle();
		assertEquals(new Location(10, -10), geofence.getBoundingBoxNorthWest());
		assertEquals(new Location(10, 10), geofence.getBoundingBoxNorthEast());
		assertEquals(new Location(-9, 10), geofence.getBoundingBoxSouthEast());
		assertEquals(new Location(-9, -10), geofence.getBoundingBoxSouthWest());
		logger.info("Bounding box calculations work properly for Dateline");
	}

	public static Geofence datelineRectangle() {
		return Geofence.polygon(Arrays.asList(
				new Location(-9, 10),
				new Location(10, 10),
				new Location(10, -10),
				new Location(-9, -10)));
	}

	public static Geofence berlinRectangle() {
		return Geofence.polygon(Arrays.asList(
				new Location(53.0, 14.0),
				new Location(53.0, 13.0),
				new Location(52.0, 13.0),
				new Location(52.0, 14.0)));
	}

	public static Geofence berlinTriangle() {
		return Geofence.polygon(Arrays.asList(
				new Location(54, 12.0),
				new Location(52, 15.0),
				new Location(50, 12.0)));
	}

}
