package de.hasenburg.geobroker.commons.model;

import de.hasenburg.geobroker.commons.model.disgb.BrokerArea;
import de.hasenburg.geobroker.commons.model.disgb.BrokerInfo;
import de.hasenburg.geobroker.commons.model.spatial.Geofence;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.*;

@SuppressWarnings({"OptionalGetWithoutIsPresent"})
public class BrokerAreaTest {

	private static final Logger logger = LogManager.getLogger();

	@Test
	public void testSerialize() {
		BrokerInfo brokerInfo = new BrokerInfo("brokerId", "address", 1000);
		BrokerArea brokerArea1 = new BrokerArea(brokerInfo, Geofence.circle(Location.random(), 10));
		KryoSerializer kryo = new KryoSerializer();
		byte[] arr = kryo.write(brokerArea1);
		logger.info(arr);
		BrokerArea brokerArea2 = kryo.read(arr, BrokerArea.class);
		assertEquals(brokerArea1, brokerArea2);
	}

	@Test
	public void testContainsLocation() {
		BrokerInfo brokerInfo = new BrokerInfo("brokerId", "address", 1000);
		Location in = new Location(10, 10);
		Location out = new Location(30, 30);
		BrokerArea brokerArea = new BrokerArea(brokerInfo, Geofence.circle(in, 10));

		assertTrue(brokerArea.containsLocation(in));
		assertFalse(brokerArea.containsLocation(out));
	}

	@Test
	public void testResponsibleBroker() {
		String ownId = "repBroker";
		BrokerInfo responsibleBroker = new BrokerInfo(ownId, "address", 1000);
		BrokerInfo otherBroker = new BrokerInfo("otherBroker", "address", 1000);

		BrokerArea brokerArea = new BrokerArea(responsibleBroker, Geofence.circle(Location.random(), 10));

		// check broker info
		assertEquals(responsibleBroker, brokerArea.getResponsibleBroker());
		assertNotEquals(otherBroker, brokerArea.getResponsibleBroker());

		// use check method
		assertTrue(brokerArea.hasResponsibleBroker(responsibleBroker.getBrokerId()));
		assertFalse(brokerArea.hasResponsibleBroker(otherBroker.getBrokerId()));
	}

	@Test
	public void testWorldResponsibility() {
		BrokerInfo brokerInfo = new BrokerInfo("brokerId", "address", 1000);
		BrokerArea brokerArea = new BrokerArea(brokerInfo, Geofence.world());

		logger.info(brokerArea);

		for (int i = 0; i < 100; i++) {
			assertTrue(brokerArea.containsLocation(Location.random()));
		}
	}

}
