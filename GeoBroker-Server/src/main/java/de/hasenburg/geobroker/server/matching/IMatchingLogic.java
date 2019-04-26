package de.hasenburg.geobroker.server.matching;

import de.hasenburg.geobroker.server.communication.InternalServerMessage;
import org.zeromq.ZMQ.Socket;

/**
 * Message Processing Notes <br>
 * 	- we already validated the messages above using #buildMessage() <br>
 * 	-> we expect the payload to be compatible with the control packet type <br>
 * 	-> we expect all fields to be set
 */
public interface IMatchingLogic {

	void processCONNECT(InternalServerMessage message, Socket clients, Socket brokers);

	void processDISCONNECT(InternalServerMessage message, Socket clients, Socket brokers);

	void processPINGREQ(InternalServerMessage message, Socket clients, Socket brokers);

	void processSUBSCRIBE(InternalServerMessage message, Socket clients, Socket brokers);

	void processUNSUBSCRIBE(InternalServerMessage message, Socket clients, Socket brokers);

	void processPUBLISH(InternalServerMessage message, Socket clients, Socket brokers);

	void processBrokerForwardPublish(InternalServerMessage message, Socket clients, Socket brokers);

}
