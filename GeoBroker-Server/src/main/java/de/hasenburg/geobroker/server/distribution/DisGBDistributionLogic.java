package de.hasenburg.geobroker.server.distribution;

import org.zeromq.ZMsg;
import org.zeromq.ZMQ.Socket;

public class DisGBDistributionLogic implements IDistributionLogic {

	/**
	 * Expects msg to be in a certain format, but does not check for it as only used internally for performance
	 * reasons. If sending something else, the receiving broker will discard the message.
	 *
	 * @param msg - this should equal an {@link de.hasenburg.geobroker.server.communication.InternalBrokerMessage}
	 * @param broker - socket that can be used to communicate with the other broker.
	 */
	@Override
	public void sendMessageToOtherBrokers(ZMsg msg, Socket broker) {
		// TODO wait and check for acknowledgements
		msg.send(broker);
	}

	@Override
	public void processOtherBrokerAcknowledgement(ZMsg msg) {
		// TODO process acknowledgement
	}
}
