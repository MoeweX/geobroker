package de.hasenburg.geobroker.server.communication;

import de.hasenburg.geobroker.commons.communication.ZMQProcessManager;
import de.hasenburg.geobroker.server.main.Configuration;
import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.server.storage.client.ClientDirectory;
import de.hasenburg.geobroker.commons.exceptions.CommunicatorException;
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ZMQProcessManagerTest {

	private static final Logger logger = LogManager.getLogger();

	@Test
	public void tearUpTearDown() throws CommunicatorException {
		// prepare
		ClientDirectory clientDirectory = new ClientDirectory();
		TopicAndGeofenceMapper topicAndGeofenceMapper = new TopicAndGeofenceMapper(new Configuration());
		ZMQProcessManager pm = new ZMQProcessManager();
		assertTrue(pm.getIncompleteZMQProcesses().isEmpty());

		// start two processes
		ZMQProcessStarter.runZMQProcess_MessageProcessor(pm,"Process 1", clientDirectory, topicAndGeofenceMapper);
		ZMQProcessStarter.runZMQProcess_MessageProcessor(pm,"Process 2", clientDirectory, topicAndGeofenceMapper);
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
