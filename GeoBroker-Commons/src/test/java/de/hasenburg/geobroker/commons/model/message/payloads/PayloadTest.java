package de.hasenburg.geobroker.commons.model.message.payloads;

import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.exceptions.CommunicatorException;
import de.hasenburg.geobroker.commons.model.JSONable;
import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.message.Topic;
import de.hasenburg.geobroker.commons.model.spatial.Geofence;
import de.hasenburg.geobroker.commons.model.spatial.Location;
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

	@Test
	public void testPUBLISHPayload() throws CommunicatorException {
		logger.info("RUNNING testPUBLISHPayload TEST");
		PUBLISHPayload payload = new PUBLISHPayload(new Topic("data"),
													Geofence.circle(Location.random(), 1),
													Utility.generatePayloadWithSize(20, "test-"));
		String json = JSONable.toJSON(payload);
		logger.debug(json);

		ZMsg message = ZMsg.newStringMsg(json);
		String messageString = message.pop().getString(ZMQ.CHARSET);

		AbstractPayload payload2 = Utility.buildPayloadFromString(messageString, ControlPacketType.PUBLISH);
		logger.debug(payload2);

		assertEquals(payload, payload2.getPUBLISHPayload().get());
		logger.info("FINISHED TEST");
	}

}
