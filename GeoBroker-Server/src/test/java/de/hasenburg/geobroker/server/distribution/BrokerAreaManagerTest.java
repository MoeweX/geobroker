package de.hasenburg.geobroker.server.distribution;

import de.hasenburg.geobroker.commons.model.spatial.Location;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.*;

@SuppressWarnings({"OptionalGetWithoutIsPresent"})
public class BrokerAreaManagerTest {

	private static final Logger logger = LogManager.getLogger();

	@Test
	public void testSetupDefault() {
		Location location = new Location(0, 0);

		BrokerAreaManager brokerAreaManager = new BrokerAreaManager("broker");
		brokerAreaManager.setup_DefaultFile();

		// we are responsible
		assertTrue(brokerAreaManager.checkIfResponsibleForClientLocation(location));
		// for everything
		assertTrue(brokerAreaManager.checkIfResponsibleForClientLocation(Location.random()));

		// as the areas overlap in the default config, two brokers are responsible in theory.
		BrokerInfo otherBroker = brokerAreaManager.getOtherBrokerForClientLocation(location);
		assertNotNull(otherBroker);
		assertEquals("notUsedBroker", otherBroker.getBrokerId());
	}

}
