package de.hasenburg.geofencebroker.model;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.main.Utility;
import de.hasenburg.geofencebroker.model.exceptions.CommunicatorException;
import de.hasenburg.geofencebroker.model.payload.AbstractPayload;
import de.hasenburg.geofencebroker.model.payload.CONNECTPayload;
import de.hasenburg.geofencebroker.model.payload.PINGREQPayload;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("ConstantConditions")
public class PayloadTest {

	private static final Logger logger = LogManager.getLogger();

	@Test
	public void testCONNECTPayload() throws CommunicatorException {
		logger.info("RUNNING testCONNECTPayload TEST");
		CONNECTPayload payload = new CONNECTPayload();
		String json = JSONable.toJSON(payload);
		logger.debug(json);

		ZMsg message = ZMsg.newStringMsg(json);
		String messageString = message.pop().getString(ZMQ.CHARSET);

		AbstractPayload payload2 = Utility.buildPayloadFromString(messageString, ControlPacketType.CONNECT);
		logger.debug(payload2);

		assertEquals(payload, payload2.getCONNECTPayload().get());
		logger.info("FINISHED TEST");
	}

	@Test
	public void testPINGREQPayload() throws CommunicatorException {
		logger.info("RUNNING testPINGREQPayload TEST");
		PINGREQPayload payload = new PINGREQPayload(Location.random());
		String json = JSONable.toJSON(payload);
		logger.debug(json);

		ZMsg message = ZMsg.newStringMsg(json);
		String messageString = message.pop().getString(ZMQ.CHARSET);

		AbstractPayload payload2 = Utility.buildPayloadFromString(messageString, ControlPacketType.PINGREQ);
		logger.debug(payload2);

		assertEquals(payload, payload2.getPINGREQPayload().get());
		logger.info("FINISHED TEST");
	}

	// cause some fields are null
	@Test(expected = CommunicatorException.class)
	public void testPINGREQPayloadEmpty() throws CommunicatorException {
		logger.info("RUNNING testPINGREQPayloadEmpty TEST");
		PINGREQPayload payload = new PINGREQPayload();
		String json = JSONable.toJSON(payload);
		logger.debug(json);

		ZMsg message = ZMsg.newStringMsg(json);
		String messageString = message.pop().getString(ZMQ.CHARSET);

		AbstractPayload payload2 = Utility.buildPayloadFromString(messageString, ControlPacketType.PINGREQ);
		logger.info("FINISHED TEST");
	}

}
