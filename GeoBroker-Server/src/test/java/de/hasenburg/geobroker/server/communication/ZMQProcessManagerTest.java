package de.hasenburg.geobroker.server.communication;

import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.communication.ZMQControlUtility;
import de.hasenburg.geobroker.commons.communication.ZMQProcessManager;
import de.hasenburg.geobroker.commons.exceptions.CommunicatorException;
import de.hasenburg.geobroker.server.main.*;
import de.hasenburg.geobroker.server.matching.SingleGeoBrokerMatchingLogic;
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper;
import de.hasenburg.geobroker.server.storage.client.ClientDirectory;
import io.prometheus.client.CollectorRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ZMQProcessManagerTest {

	private static final Logger logger = LogManager.getLogger();

	@Before
	public void setUpTest() {
		CollectorRegistry.defaultRegistry.clear();
	}

	@Test
	public void tearUpTearDown() throws CommunicatorException {
		// prepare
		ClientDirectory clientDirectory = new ClientDirectory();
		TopicAndGeofenceMapper topicAndGeofenceMapper = new TopicAndGeofenceMapper(new Configuration());

		SingleGeoBrokerMatchingLogic matchingLogic = new SingleGeoBrokerMatchingLogic(clientDirectory,
				topicAndGeofenceMapper);

		ZMQProcessManager pm = new ZMQProcessManager();
		assertTrue(pm.getIncompleteZMQProcesses().isEmpty());

		// start two processes
		ZMQProcessStarter.runZMQProcess_MessageProcessor(pm, "test", 1, matchingLogic);
		ZMQProcessStarter.runZMQProcess_MessageProcessor(pm, "test", 2, matchingLogic);
		Utility.sleepNoLog(100, 0);
		assertTrue(pm.getIncompleteZMQProcesses()
					 .containsAll(Arrays.asList(ZMQProcess_MessageProcessorKt.getMessageProcessorIdentity("test", 1),
							 ZMQProcess_MessageProcessorKt.getMessageProcessorIdentity("test", 2))));
		logger.info("Started two message processor processes");
		Utility.sleepNoLog(100, 0);

		logger.info("Sending kill to processes");
		// kill 1
		pm.sendCommandToZMQProcess(ZMQProcess_MessageProcessorKt.getMessageProcessorIdentity("test", 1), ZMQControlUtility.ZMQControlCommand.KILL);

		Utility.sleepNoLog(100, 0);
		assertFalse(pm.getIncompleteZMQProcesses().contains(ZMQProcess_MessageProcessorKt.getMessageProcessorIdentity("test", 1)));
		assertTrue(pm.getIncompleteZMQProcesses().contains(ZMQProcess_MessageProcessorKt.getMessageProcessorIdentity("test", 2)));
		logger.info("Killed first message processor processes");

		// tear down
		assertTrue(pm.tearDown(5000));
		logger.info("Killed both message processor processes");
	}

}
