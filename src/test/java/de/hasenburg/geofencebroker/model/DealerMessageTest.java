package de.hasenburg.geofencebroker.model;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.model.geofence.Geofence;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.zeromq.ZMsg;
import zmq.socket.reqrep.Dealer;

import static org.junit.Assert.assertEquals;
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
		DealerMessage message = new DealerMessage(ControlPacketType.CONNECT);
		logger.debug(message);
		ZMsg zmsg = message.getZmsg();
		DealerMessage message2 = DealerMessage.buildDealerMessage(zmsg).get();
		logger.debug(message2);
		assertEquals("Messages should be equal", message, message2);
		logger.info("FINISHED TEST");
	}

	@Test
	public void testPayloadPINGREQ() {
		logger.info("RUNNING testPayloadPINGREQ TEST");
		DealerMessage message = new DealerMessage(ControlPacketType.PINGREQ, new PayloadPINGREQ(Location.random()));
		logger.debug(message);
		ZMsg zmsg = message.getZmsg();
		DealerMessage message2 = DealerMessage.buildDealerMessage(zmsg).get();
		logger.debug(message2);
		assertEquals("Messages should be equal", message, message2);
		logger.info("FINISHED TEST");
	}

	@Test
	public void testGeofenceCIRCLE() {
		logger.info("RUNNING testGeofenceCIRCLE TEST");
		Geofence geofence = new Geofence();
		geofence.buildCircle(Location.random(), 20.0);
		DealerMessage message = new DealerMessage(ControlPacketType.PUBLISH, new Topic("topic"), geofence, new Payload());
		logger.debug(message);
		ZMsg zmsg = message.getZmsg();
		DealerMessage message2 = DealerMessage.buildDealerMessage(zmsg).get();
		logger.debug(message2);
		assertEquals("Messages should be equal", message, message2);
		logger.info("FINISHED TEST");
	}
}
