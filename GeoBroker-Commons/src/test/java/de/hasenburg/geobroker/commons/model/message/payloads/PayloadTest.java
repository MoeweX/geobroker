package de.hasenburg.geobroker.commons.model.message.payloads;

import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.exceptions.CommunicatorException;
import de.hasenburg.geobroker.commons.model.KryoSerializer;
import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.message.Topic;
import de.hasenburg.geobroker.commons.model.spatial.Geofence;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.zeromq.ZMsg;

import static org.junit.Assert.*;

@SuppressWarnings("ConstantConditions")
public class PayloadTest {

	private static final Logger logger = LogManager.getLogger();
	private static KryoSerializer kryo = new KryoSerializer();

	@Test
	public void testCONNECTPayload() {
		CONNECTPayload payload = new CONNECTPayload(Location.random());
		byte[] bytes1 = kryo.write(payload);

		ZMsg message = new ZMsg().addFirst(bytes1);
		byte[] bytes2 = message.getFirst().getData();

		CONNECTPayload payload2 = kryo.read(bytes2, ControlPacketType.CONNECT).getCONNECTPayload();
		assertEquals(payload, payload2);
	}

	@Test
	public void testPINGREQPayload() {
		PINGREQPayload payload = new PINGREQPayload(Location.random());
		byte[] bytes1 = kryo.write(payload);

		ZMsg message = new ZMsg().addFirst(bytes1);
		byte[] bytes2 = message.getFirst().getData();

		PINGREQPayload payload2 = kryo.read(bytes2, ControlPacketType.PINGREQ).getPINGREQPayload();
		assertEquals(payload, payload2);
	}

	@Test (expected = IllegalArgumentException.class)
	public void testPINGREQPayloadEmpty() {
		PINGREQPayload payload = new PINGREQPayload(null);
		byte[] bytes1 = kryo.write(payload);

		ZMsg message = new ZMsg().addFirst(bytes1);
		byte[] bytes2 = message.getFirst().getData();

		assertNull(kryo.read(bytes2, ControlPacketType.PINGREQ));
	}

	@Test
	public void testPUBLISHPayload() {
		logger.info("RUNNING testPUBLISHPayload TEST");
		PUBLISHPayload payload = new PUBLISHPayload(new Topic("data"),
				Geofence.circle(Location.random(), 1),
				Utility.generatePayloadWithSize(20, "test-"));
		byte[] bytes1 = kryo.write(payload);

		ZMsg message = new ZMsg().addFirst(bytes1);
		byte[] bytes2 = message.getFirst().getData();

		PUBLISHPayload payload2 = kryo.read(bytes2, ControlPacketType.PUBLISH).getPUBLISHPayload();
		assertEquals(payload, payload2);
	}

	@Test
	public void testSUBSCRIBEPayload() {
		SUBSCRIBEPayload payload = new SUBSCRIBEPayload(new Topic("data"), Geofence.circle(Location.random(), 1));
		byte[] bytes = kryo.write(payload);
		AbstractPayload payload2 = kryo.read(bytes, ControlPacketType.SUBSCRIBE);
		assertEquals(payload, payload2.getSUBSCRIBEPayload());
	}


	@Test
	public void testBrokerForwardPublishPayload() {
		// publisher matching
		BrokerForwardPublishPayload payload = new BrokerForwardPublishPayload(new PUBLISHPayload(new Topic("data"),
				Geofence.circle(Location.random(), 1.0),
				"Some random content"), "Subscriber 1");
		byte[] bytes = kryo.write(payload);
		AbstractPayload payload2 = kryo.read(bytes, ControlPacketType.BrokerForwardPublish);
		assertEquals(payload, payload2.getBrokerForwardPublishPayload());

		// TODO subscriber matching
	}
}
