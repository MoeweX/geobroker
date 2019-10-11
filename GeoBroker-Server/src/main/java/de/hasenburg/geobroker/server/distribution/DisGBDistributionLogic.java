package de.hasenburg.geobroker.server.distribution;

import de.hasenburg.geobroker.commons.model.KryoSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;
import de.hasenburg.geobroker.commons.model.message.Payload;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static de.hasenburg.geobroker.commons.model.message.Payload.*;

/**
 * As any {@link Payload} does not have an id, this class simply counts outgoing and ingoing (acknowledged)
 * messages.
 *
 * An alternative to adding an ID to every payload would be to add an ID only to BrokerForward payloads, e.g., {@link
 * BrokerForwardPublishPayload}.
 */
public class DisGBDistributionLogic implements IDistributionLogic {

	private static final Logger logger = LogManager.getLogger();

	private ConcurrentHashMap<String, AtomicInteger> notAcknowledgedMessages = new ConcurrentHashMap<>();

	/**
	 * Expects msg to be in a certain format, but does not check for it as only used internally for performance reasons.
	 * If sending something else, the receiving broker will discard the message.
	 *
	 * Also increments the not acknowledged counter by 1.
	 *
	 * @param msg - this should equal the ZMsg of a {@link Payload}
	 * @param broker - socket that can be used to communicate with the other broker.
	 * @param targetBrokerId - id of the other broker we are sending this message to
	 */
	@Override
	public void sendMessageToOtherBrokers(ZMsg msg, Socket broker, String targetBrokerId, KryoSerializer kryo) {
		int n = notAcknowledgedMessages.computeIfAbsent(targetBrokerId, k -> new AtomicInteger(0)).incrementAndGet();
		logger.trace("New message sent to other broker, now {} not acknowledged messages", n);
		msg.send(broker);
	}

	/**
	 * Decrements the not acknowledged counter by 1.
	 *
	 * @param msg - for now this can be anything as we do not check ids
	 * @param otherBrokerId - id of the other broker that sent us the acknowledgement
	 */
	@Override
	public void processOtherBrokerAcknowledgement(ZMsg msg, String otherBrokerId, KryoSerializer kryo) {
		if (notAcknowledgedMessages.containsKey(otherBrokerId)) {
			int n = notAcknowledgedMessages.get(otherBrokerId).decrementAndGet();
			logger.trace("Other broker acknowledged a {} message, now {} not acknowledged messages", msg.getFirst(), n);
		}
	}

	public ConcurrentHashMap<String, AtomicInteger> getNotAcknowledgedMessages() {
		return notAcknowledgedMessages;
	}
}
