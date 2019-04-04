package de.hasenburg.geobroker.server.distribution;

import de.hasenburg.geobroker.commons.model.BrokerInfo;
import de.hasenburg.geobroker.commons.model.JSONable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@SuppressWarnings({"OptionalGetWithoutIsPresent"})
public class BrokerInfoTest {

	private static final Logger logger = LogManager.getLogger();

	@Test
	public void testSerialize() {
		BrokerInfo brokerInfo1 = new BrokerInfo("brokerId", "address", 1000);
		String json = JSONable.toJSON(brokerInfo1);
		logger.info(json);
		BrokerInfo brokerInfo2 = JSONable.fromJSON(json, BrokerInfo.class).get();
		assertEquals(brokerInfo1, brokerInfo2);
	}

}
