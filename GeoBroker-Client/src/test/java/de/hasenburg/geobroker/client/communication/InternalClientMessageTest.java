package de.hasenburg.geobroker.client.communication;

import de.hasenburg.geobroker.commons.model.KryoSerializer;
import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.message.payloads.CONNECTPayload;
import de.hasenburg.geobroker.commons.model.message.payloads.PINGREQPayload;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.zeromq.ZMsg;

import static org.junit.Assert.*;

@SuppressWarnings("ConstantConditions")
public class InternalClientMessageTest {

	private static final Logger logger = LogManager.getLogger();
	private KryoSerializer kryo;

	@Before
	public void setUp() {
		kryo = new KryoSerializer();
	}

	/*****************************************************************
	 * Tests to evaluate if payloads are transmitted correctly
	 ****************************************************************/

	@Test
	public void testPayloadConnect() {
		logger.info("RUNNING testPayloadConnect TEST");
		InternalClientMessage message = new InternalClientMessage(ControlPacketType.CONNECT, new CONNECTPayload(Location.random()));
		logger.debug(message);
		ZMsg zmsg = message.getZMsg(kryo);
		InternalClientMessage message2 = InternalClientMessage.buildMessage(zmsg, kryo);
		logger.debug(message2);
		assertEquals("Messages should be equal", message, message2);
		logger.info("FINISHED TEST");
	}

	@Test
	public void testPayloadPINGREQ() {
		logger.info("RUNNING testPayloadPINGREQ TEST");
		InternalClientMessage message = new InternalClientMessage(ControlPacketType.PINGREQ, new PINGREQPayload(Location.random()));
		logger.debug(message);
		ZMsg zmsg = message.getZMsg(kryo);
		InternalClientMessage message2 = InternalClientMessage.buildMessage(zmsg, kryo);
		logger.debug(message2);
		assertEquals("Messages should be equal", message, message2);
		logger.info("FINISHED TEST");
	}

	@Test (expected = IllegalArgumentException.class)
	public void testPayloadPINGREQEmpty() {
		logger.info("RUNNING testPayloadPINGREQEmpty TEST");
		InternalClientMessage message = new InternalClientMessage(ControlPacketType.PINGREQ, new PINGREQPayload(null));
		logger.debug(message);
		ZMsg zmsg = message.getZMsg(kryo);
		assertNull(InternalClientMessage.buildMessage(zmsg, kryo));
		logger.info("FINISHED TEST");
	}

}
