package de.hasenburg.geofencebroker.model;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.model.payload.CONNECTPayload;
import de.hasenburg.geofencebroker.model.payload.PINGREQPayload;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.zeromq.ZMsg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@SuppressWarnings("ConstantConditions")
public class DealerMessageTest {

	private static final Logger logger = LogManager.getLogger();

	/*****************************************************************
	 * Tests to evaluate if payloads are transmitted correctly
	 ****************************************************************/

	@Test
	public void testPayloadConnect() {
		logger.info("RUNNING testPayloadConnect TEST");
		DealerMessage message = new DealerMessage(ControlPacketType.CONNECT, new CONNECTPayload());
		logger.debug(message);
		ZMsg zmsg = message.getZMsg();
		DealerMessage message2 = DealerMessage.buildDealerMessage(zmsg).get();
		logger.debug(message2);
		assertEquals("Messages should be equal", message, message2);
		logger.info("FINISHED TEST");
	}

	@Test
	public void testPayloadPINGREQ() {
		logger.info("RUNNING testPayloadPINGREQ TEST");
		DealerMessage message = new DealerMessage(ControlPacketType.PINGREQ, new PINGREQPayload(Location.random()));
		logger.debug(message);
		ZMsg zmsg = message.getZMsg();
		DealerMessage message2 = DealerMessage.buildDealerMessage(zmsg).get();
		logger.debug(message2);
		assertEquals("Messages should be equal", message, message2);
		logger.info("FINISHED TEST");
	}

	@Test
	public void testPayloadPINGREQEmpty() {
		logger.info("RUNNING testPayloadPINGREQEmpty TEST");
		DealerMessage message = new DealerMessage(ControlPacketType.PINGREQ, new PINGREQPayload());
		logger.debug(message);
		ZMsg zmsg = message.getZMsg();
		assertFalse(DealerMessage.buildDealerMessage(zmsg).isPresent());
		logger.info("FINISHED TEST");
	}

}
