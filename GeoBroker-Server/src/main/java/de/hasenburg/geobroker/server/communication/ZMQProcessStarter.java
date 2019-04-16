package de.hasenburg.geobroker.server.communication;

import de.hasenburg.geobroker.commons.communication.ZMQProcessManager;
import de.hasenburg.geobroker.commons.model.BrokerInfo;
import de.hasenburg.geobroker.server.distribution.BrokerAreaManager;
import de.hasenburg.geobroker.server.distribution.IDistributionLogic;
import de.hasenburg.geobroker.server.matching.IMatchingLogic;
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper;
import de.hasenburg.geobroker.server.storage.client.ClientDirectory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class ZMQProcessStarter {

	private static final Logger logger = LogManager.getLogger();

	public static ZMQProcess_Server runZMQProcess_Server(ZMQProcessManager processManager, String address, int port,
														 String identity) {
		ZMQProcess_Server zmqProcess = new ZMQProcess_Server(address, port, identity);
		processManager.submitZMQProcess(identity, zmqProcess);
		return zmqProcess;
	}

	public static ZMQProcess_MessageProcessor runZMQProcess_MessageProcessor(ZMQProcessManager processManager,
																			 String identity,
																			 IMatchingLogic matchingLogic) {
		ZMQProcess_MessageProcessor zmqProcess = new ZMQProcess_MessageProcessor(identity, matchingLogic);
		processManager.submitZMQProcess(identity, zmqProcess);
		return zmqProcess;
	}

	public static ZMQProcess_MessageProcessor runZMQProcess_MessageProcessor(ZMQProcessManager processManager,
																			 String identity,
																			 IMatchingLogic matchingLogic,
																			 List<String> brokerCommunicatorAddresses) {
		ZMQProcess_MessageProcessor zmqProcess =
				new ZMQProcess_MessageProcessor(identity, matchingLogic, brokerCommunicatorAddresses);
		processManager.submitZMQProcess(identity, zmqProcess);
		return zmqProcess;
	}

	public static ZMQProcess_BrokerCommunicator runZMQProcess_BrokerCommunicator(ZMQProcessManager processManager,
																				 String identity,
																				 IDistributionLogic distributionLogic,
																				 List<BrokerInfo> otherBrokerInfos) {
		ZMQProcess_BrokerCommunicator zmqProcess =
				new ZMQProcess_BrokerCommunicator(identity, distributionLogic, otherBrokerInfos);
		processManager.submitZMQProcess(identity, zmqProcess);
		return zmqProcess;
	}

}
