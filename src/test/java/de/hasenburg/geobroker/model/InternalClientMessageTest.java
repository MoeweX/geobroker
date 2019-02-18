package de.hasenburg.geobroker.model;

import de.hasenburg.geobroker.communication.ControlPacketType;
import de.hasenburg.geobroker.model.payload.CONNECTPayload;
import de.hasenburg.geobroker.model.payload.PINGREQPayload;
import de.hasenburg.geobroker.model.spatial.Location;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.zeromq.ZMsg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@SuppressWarnings("ConstantConditions")
public class InternalClientMessageTest {

	private static final Logger logger = LogManager.getLogger();

	/*****************************************************************
	 * Tests to evaluate if payloads are transmitted correctly
	 ****************************************************************/

	@Test
	public void testPayloadConnect() {
		logger.info("RUNNING testPayloadConnect TEST");
		InternalClientMessage message = new InternalClientMessage(ControlPacketType.CONNECT, new CONNECTPayload(Location.random()));
		logger.debug(message);
		ZMsg zmsg = message.getZMsg();
		InternalClientMessage message2 = InternalClientMessage.buildMessage(zmsg).get();
		logger.debug(message2);
		assertEquals("Messages should be equal", message, message2);
		logger.info("FINISHED TEST");
	}

	@Test
	public void testPayloadPINGREQ() {
		logger.info("RUNNING testPayloadPINGREQ TEST");
		InternalClientMessage message = new InternalClientMessage(ControlPacketType.PINGREQ, new PINGREQPayload(Location.random()));
		logger.debug(message);
		ZMsg zmsg = message.getZMsg();
		InternalClientMessage message2 = InternalClientMessage.buildMessage(zmsg).get();
		logger.debug(message2);
		assertEquals("Messages should be equal", message, message2);
		logger.info("FINISHED TEST");
	}

	@Test
	public void testPayloadPINGREQEmpty() {
		logger.info("RUNNING testPayloadPINGREQEmpty TEST");
		InternalClientMessage message = new InternalClientMessage(ControlPacketType.PINGREQ, new PINGREQPayload());
		logger.debug(message);
		ZMsg zmsg = message.getZMsg();
		assertFalse(InternalClientMessage.buildMessage(zmsg).isPresent());
		logger.info("FINISHED TEST");
	}

}