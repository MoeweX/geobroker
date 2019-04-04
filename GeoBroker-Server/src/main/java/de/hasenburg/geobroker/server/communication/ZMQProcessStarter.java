package de.hasenburg.geobroker.server.communication;

import de.hasenburg.geobroker.commons.communication.ZMQProcessManager;
import de.hasenburg.geobroker.server.distribution.BrokerAreaManager;
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper;
import de.hasenburg.geobroker.server.storage.client.ClientDirectory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ZMQProcessStarter {

	private static final Logger logger = LogManager.getLogger();

	public static void runZMQProcess_MessageProcessor(ZMQProcessManager processManager, String identity,
													  ClientDirectory clientDirectory,
													  TopicAndGeofenceMapper topicAndGeofenceMapper,
													  BrokerAreaManager brokerAreaManager) {
		processManager.submitZMQProcess(identity,
										new ZMQProcess_MessageProcessor(identity,
																		clientDirectory,
																		topicAndGeofenceMapper,
																		brokerAreaManager));
	}

	public static void runZMQProcess_Server(ZMQProcessManager processManager, String address, int port,
											String identity) {
		processManager.submitZMQProcess(identity, new ZMQProcess_Server(address, port, identity));
	}

}
