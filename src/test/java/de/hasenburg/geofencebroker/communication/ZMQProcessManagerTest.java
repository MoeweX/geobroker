package de.hasenburg.geofencebroker.communication;

import de.hasenburg.geofencebroker.main.Utility;
import de.hasenburg.geofencebroker.model.connections.ConnectionManager;
import de.hasenburg.geofencebroker.model.exceptions.CommunicatorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ZMQProcessManagerTest {

	private static final Logger logger = LogManager.getLogger();

	// TODO: Fix when resolved: https://github.com/zeromq/jeromq/issues/595
	@Ignore
	@Test
	public void tearUpTearDown() throws CommunicatorException {
		// prepare
		ConnectionManager cm = new ConnectionManager();
		ZMQProcessManager pm = new ZMQProcessManager();
		assertTrue(pm.getIncompleteZMQProcesses().isEmpty());

		// start two processes
		pm.runZMQProcess_MessageProcessor("Process 1", cm);
		pm.runZMQProcess_MessageProcessor("Process 2", cm);
		Utility.sleepNoLog(100, 0);
		assertTrue(pm.getIncompleteZMQProcesses().containsAll(Arrays.asList("Process 1", "Process 2")));
		logger.info("Started two message processor processes");

		// kill 1
		pm.sendKillCommandToZMQProcess("Process 1");
		Utility.sleepNoLog(100, 0);
		assertFalse(pm.getIncompleteZMQProcesses().contains("Process 1"));
		assertTrue(pm.getIncompleteZMQProcesses().contains("Process 2"));
		logger.info("Killed first message processor processes");

		// tear down
		assertTrue(pm.tearDown(5000));
		logger.info("Killed both message processor processes");
	}

}
