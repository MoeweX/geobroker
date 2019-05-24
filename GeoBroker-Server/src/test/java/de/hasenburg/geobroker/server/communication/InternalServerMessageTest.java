package de.hasenburg.geobroker.server.communication;

import de.hasenburg.geobroker.commons.model.KryoSerializer;
import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.message.Topic;
import de.hasenburg.geobroker.commons.model.message.payloads.BrokerForwardPublishPayload;
import de.hasenburg.geobroker.commons.model.message.payloads.PUBLISHPayload;
import de.hasenburg.geobroker.commons.model.spatial.Geofence;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.zeromq.ZMsg;

import static org.junit.Assert.assertEquals;

@SuppressWarnings({"OptionalGetWithoutIsPresent"})
public class InternalServerMessageTest {

    private static final Logger logger = LogManager.getLogger();

    @Test
    public void testValid() {
        PUBLISHPayload publishPayload = new PUBLISHPayload(new Topic("topic"), Geofence.world(), "content");
        BrokerForwardPublishPayload bfpp = new BrokerForwardPublishPayload(publishPayload, Location.random());

        InternalServerMessage valid = new InternalServerMessage("Client", ControlPacketType.BrokerForwardPublish, bfpp);

        ZMsg validZMsg = valid.getZMsg(new KryoSerializer());
        logger.info(validZMsg);

        assertEquals(valid, InternalServerMessage.buildMessage(validZMsg, new KryoSerializer()).get());
    }

}