package de.hasenburg.geofencebroker;

import de.hasenburg.geofencebroker.communication.Broker;
import de.hasenburg.geofencebroker.model.Location;
import de.hasenburg.geofencebroker.model.exceptions.CommunicatorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BrokerTest {

	private static final Logger logger = LogManager.getLogger();

	@Test
	public void tearUpTearDown() throws CommunicatorException {
		logger.info("RUNNING tearUpTearDown TEST");

		Broker broker = new Broker("tcp://localhost", 5559);
		broker.init();
		assertTrue(broker.isBrokering());
		broker.tearDown();

		logger.info("FINISHED TEST");
	}


}
