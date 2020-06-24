package de.hasenburg.geobroker.server.communication;

import de.hasenburg.geobroker.commons.communication.ZMQProcessManager;
import de.hasenburg.geobroker.server.matching.IMatchingLogic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ZMQProcessStarter {

	private static final Logger logger = LogManager.getLogger();

	public static ZMQProcess_Server runZMQProcess_Server(ZMQProcessManager processManager, String ip, int port,
														 String brokerId) {
		ZMQProcess_Server zmqProcess = new ZMQProcess_Server(ip, port, brokerId);
		processManager.submitZMQProcess(ZMQProcess_Server.getServerIdentity(brokerId), zmqProcess);
		return zmqProcess;
	}

	public static ZMQProcess_MessageProcessor runZMQProcess_MessageProcessor(ZMQProcessManager processManager,
																			 String brokerId, int number,
																			 IMatchingLogic matchingLogic) {
		ZMQProcess_MessageProcessor zmqProcess = new ZMQProcess_MessageProcessor(brokerId,
				number,
				matchingLogic);
		processManager.submitZMQProcess(ZMQProcess_MessageProcessorKt.getMessageProcessorIdentity(brokerId, number),
				zmqProcess);
		return zmqProcess;
	}

}
