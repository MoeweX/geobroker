package de.hasenburg.geobroker.model;

import de.hasenburg.geobroker.communication.ControlPacketType;
import de.hasenburg.geobroker.main.Utility;
import de.hasenburg.geobroker.model.exceptions.CommunicatorException;
import de.hasenburg.geobroker.model.payload.AbstractPayload;
import de.hasenburg.geobroker.model.payload.CONNECTPayload;
import de.hasenburg.geobroker.model.payload.PINGREQPayload;
import de.hasenburg.geobroker.model.spatial.Location;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("ConstantConditions")
public class PayloadTest {

	private static final Logger logger = LogManager.getLogger();

	@Test
	public void testCONNECTPayload() throws CommunicatorException {
		logger.info("RUNNING testCONNECTPayload TEST");
		CONNECTPayload payload = new CONNECTPayload(Location.random());
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
