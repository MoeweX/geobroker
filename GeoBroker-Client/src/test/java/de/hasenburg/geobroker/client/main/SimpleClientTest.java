package de.hasenburg.geobroker.client.main;

import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.communication.ZMQProcessManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SimpleClientTest {

	private static final Logger logger = LogManager.getLogger();

	@Test
	public void startupTearDown() {
		ZMQProcessManager processManager = new ZMQProcessManager();
		SimpleClient simpleClient = new SimpleClient("localhost", 1, processManager, "test");
		Utility.sleepNoLog(1000, 0);
		logger.info(processManager.getContext().getSockets());
		simpleClient.tearDownClient();
		Utility.sleepNoLog(1000, 0);
		assertEquals(1, processManager.getContext().getSockets().size()); // one = PUB of process manager
		processManager.tearDown(1000);
	}
}
