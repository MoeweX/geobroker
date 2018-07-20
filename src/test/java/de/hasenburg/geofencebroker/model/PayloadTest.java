package de.hasenburg.geofencebroker.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.zeromq.ZMsg;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("ConstantConditions")
public class PayloadTest {

	private static final Logger logger = LogManager.getLogger();

	@Test
	public void testPayloadPINGREQ() {
		logger.info("RUNNING testPayloadPINGREQ TEST");
		Payload payload = new PayloadPINGREQ(Location.random());
		logger.debug(payload);
		byte[] bytes = JSONable.toJSON(payload).getBytes();

		// we cannot add just a string because of ZMsg byte encoding (not compatible)
		ZMsg message = new ZMsg();
		message.add(bytes);
		byte[] bytes2 = message.pop().getData();
		String messageString = new String(bytes);

		Payload payload2 = JSONable.fromJSON(messageString, PayloadPINGREQ.class).get();
		logger.debug(payload2);

		assertEquals(payload, payload2);
		logger.info("FINISHED TEST");
	}

	@Test
	public void testPayloadPINGREQEmpty() {
		logger.info("RUNNING testPayloadPINGREQEmpty TEST");
		Payload payload = new PayloadPINGREQ();
		logger.debug(payload);
		byte[] bytes = JSONable.toJSON(payload).getBytes();

		// we cannot add just a string because of ZMsg byte encoding (not compatible)
		ZMsg message = new ZMsg();
		message.add(bytes);
		byte[] bytes2 = message.pop().getData();
		String messageString = new String(bytes);

		Payload payload2 = JSONable.fromJSON(messageString, PayloadPINGREQ.class).get();
		logger.debug(payload2);

		assertEquals(payload, payload2);
		logger.info("FINISHED TEST");
	}
}
