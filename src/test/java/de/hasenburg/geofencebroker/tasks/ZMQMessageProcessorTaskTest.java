package de.hasenburg.geofencebroker.tasks;

import de.hasenburg.geofencebroker.communication.ZMQControlUtility;
import de.hasenburg.geofencebroker.main.Utility;
import de.hasenburg.geofencebroker.model.connections.ConnectionManager;
import de.hasenburg.geofencebroker.model.exceptions.CommunicatorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.concurrent.Future;

import static org.junit.Assert.assertTrue;

public class ZMQMessageProcessorTaskTest {

	private static final Logger logger = LogManager.getLogger();

	@Test
	public void tearUpTearDown() throws CommunicatorException {
		logger.info("RUNNING tearUpTearDown TEST");

		// prepare
		TaskManager tm = new TaskManager();
		ConnectionManager cm = new ConnectionManager();
		ZContext context = new ZContext();
		ZMQ.Socket zmqController = ZMQControlUtility.createZMQControlSocket(context);

		// run and tear down
		Future<Boolean> future = tm.runZMQMessageProcessorTask(context, cm);
		Utility.sleep(200, 0);
		ZMQControlUtility.sendZMQControlCommand(zmqController, "ZMQMessageProcessorTask", ZMQControlUtility.ZMQControlCommand.KILL);
		Utility.sleep(100, 0);
		assertTrue(future.isDone());

		// clean up
		context.destroy();
		tm.tearDown();

		logger.info("FINISHED TEST");
	}

}
