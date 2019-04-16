package de.hasenburg.geobroker.server.distribution;

import org.zeromq.ZMsg;
import org.zeromq.ZMQ.Socket;

public interface IDistributionLogic {

	void sendMessageToOtherBrokers(ZMsg msg, Socket broker);

	void processOtherBrokerAcknowledgement(ZMsg msg);

}
