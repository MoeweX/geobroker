package de.hasenburg.geobroker.server.matching.other

import de.hasenburg.geobroker.commons.model.KryoSerializer
import de.hasenburg.geobroker.commons.model.message.ControlPacketType
import de.hasenburg.geobroker.commons.model.message.ReasonCode
import de.hasenburg.geobroker.commons.model.message.payloads.PINGRESPPayload
import de.hasenburg.geobroker.commons.model.message.payloads.PUBACKPayload
import de.hasenburg.geobroker.commons.model.message.payloads.SUBACKPayload
import de.hasenburg.geobroker.commons.model.message.payloads.UNSUBACKPayload
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.server.communication.InternalServerMessage
import de.hasenburg.geobroker.server.matching.*
import de.hasenburg.geobroker.server.storage.client.ClientDirectory
import de.hasenburg.geobroker.server.storage.other.nogeo.NoGeoSubscriptionIndexingStructure
import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.logging.log4j.LogManager
import org.zeromq.ZMQ.Socket

private val logger = LogManager.getLogger()

/**
 * One GeoBroker instance that does not communicate with others and ignores all Geo information.
 * Uses the [de.hasenburg.geobroker.server.storage.other.nogeo.NoGeoSubscriptionIndexingStructure].
 *
 * As this class also relies on the geo-aware [ClientDirectory], it just puts [Geofence.world] everywhere as they are
 * ignored anyways.
 */
class SingleNoGeoMatchingLogic(private val clientDirectory: ClientDirectory,
                               private val subscriptionIndexingStructure: NoGeoSubscriptionIndexingStructure) :
    IMatchingLogic {

    private val ignore: Geofence = Geofence.world()

    override fun processCONNECT(message: InternalServerMessage, clients: Socket, brokers: Socket,
                                kryo: KryoSerializer) {
        val payload = message.payload.getCONNECTPayload() ?: return

        val response =
                connectClientAtLocalBroker(message.clientIdentifier, payload.location, clientDirectory, logger, kryo)

        logger.trace("Sending response $response")
        response.getZMsg(kryo).send(clients)
    }

    override fun processDISCONNECT(message: InternalServerMessage, clients: Socket, brokers: Socket,
                                   kryo: KryoSerializer) {
        val payload = message.payload.getDISCONNECTPayload() ?: return

        val success = clientDirectory.removeClient(message.clientIdentifier)
        if (!success) {
            logger.trace("Client for {} did not exist", message.clientIdentifier)
            return
        }

        logger.debug("Disconnected client {}, code {}", message.clientIdentifier, payload.reasonCode)

    }

    override fun processPINGREQ(message: InternalServerMessage, clients: Socket, brokers: Socket,
                                kryo: KryoSerializer) {
        val payload = message.payload.getPINGREQPayload() ?: return

        val reasonCode = updateClientLocationAtLocalBroker(message.clientIdentifier,
                payload.location,
                clientDirectory,
                logger,
                kryo)

        val response =
                InternalServerMessage(message.clientIdentifier, ControlPacketType.PINGRESP, PINGRESPPayload(reasonCode))

        logger.trace("Sending response $response")
        response.getZMsg(kryo).send(clients)
    }

    override fun processSUBSCRIBE(message: InternalServerMessage, clients: Socket, brokers: Socket,
                                  kryo: KryoSerializer) {
        val payload = message.payload.getSUBSCRIBEPayload() ?: return
        val reasonCode: ReasonCode

        val subscribed: ImmutablePair<ImmutablePair<String, Int>, Geofence>? =
                clientDirectory.checkIfSubscribed(message.clientIdentifier, payload.topic, ignore)

        if (subscribed != null) {
            // if already subscribed -> done
            reasonCode = ReasonCode.Success
        } else {
            // create subscription
            val subscriptionId = clientDirectory.updateSubscription(message.clientIdentifier, payload.topic, ignore)
            // index subscription
            reasonCode = if (subscriptionId == null) {
                logger.debug("Client {} is not connected", message.clientIdentifier)
                ReasonCode.NotConnected
            } else {
                subscriptionIndexingStructure.putSubscriptionId(subscriptionId, payload.topic)
                logger.debug("Client {} subscribed to topic {}", message.clientIdentifier, payload.topic)
                ReasonCode.GrantedQoS0
            }
        }

        val response =
                InternalServerMessage(message.clientIdentifier, ControlPacketType.SUBACK, SUBACKPayload(reasonCode))

        logger.trace("Sending response $response")
        response.getZMsg(kryo).send(clients)
    }

    override fun processUNSUBSCRIBE(message: InternalServerMessage, clients: Socket, brokers: Socket,
                                    kryo: KryoSerializer) {
        val payload = message.payload.getUNSUBSCRIBEPayload() ?: return

        val reasonCode: ReasonCode

        // unsubscribe from client directory -> get subscription id
        val s = clientDirectory.removeSubscription(message.clientIdentifier, payload.topic)

        // remove from storage if existed
        reasonCode = if (s != null) {
            subscriptionIndexingStructure.removeSubscriptionId(s.subscriptionId, s.topic)
            logger.debug("Client $message.clientIdentifier unsubscribed from topic $payload.topic, " +
                    "subscription had the id ${s.subscriptionId}")
            ReasonCode.Success
        } else {
            logger.debug("Client $message.clientIdentifier has no subscription with topic $payload.topic, " +
                    "thus unable to unsubscribe")
            ReasonCode.NoSubscriptionExisted
        }

        val response =
                InternalServerMessage(message.clientIdentifier, ControlPacketType.UNSUBACK, UNSUBACKPayload(reasonCode))

        logger.trace("Sending response $response")
        response.getZMsg(kryo).send(clients)
    }

    override fun processPUBLISH(message: InternalServerMessage, clients: Socket, brokers: Socket,
                                kryo: KryoSerializer) {
        val reasonCode: ReasonCode
        val payload = message.payload.getPUBLISHPayload() ?: return
        val publisherLocation = clientDirectory.getClientLocation(message.clientIdentifier)

        if (publisherLocation == null) { // null if client is not connected
            logger.debug("Client {} is not connected", message.clientIdentifier)
            reasonCode = ReasonCode.NotConnected
        } else {
            logger.debug("Publishing topic {} to all subscribers", payload.topic)

            // get subscriptions that match the topic
            val subscriptionIds =
                    subscriptionIndexingStructure.getSubscriptionIds(payload.topic)

            // publish message to subscribers
            for (subscriptionId in subscriptionIds) {
                val subscriberClientIdentifier = subscriptionId.left
                logger.debug("Client {} is a subscriber", subscriberClientIdentifier)
                val toPublish =
                        InternalServerMessage(subscriberClientIdentifier, ControlPacketType.PUBLISH, payload)
                logger.trace("Publishing $toPublish")
                toPublish.getZMsg(kryo).send(clients)
            }

            reasonCode = if (subscriptionIds.isEmpty()) {
                logger.debug("No subscriber exists.")
                ReasonCode.NoMatchingSubscribers
            } else {
                ReasonCode.Success
            }
        }

        // send response to publisher
        logger.trace("Sending response with reason code $reasonCode")
        val response =
                InternalServerMessage(message.clientIdentifier, ControlPacketType.PUBACK, PUBACKPayload(reasonCode))
        response.getZMsg(kryo).send(clients)
    }


    /*****************************************************************
     * Broker Forward Methods
     ****************************************************************/

    override fun processBrokerForwardDisconnect(message: InternalServerMessage, clients: Socket, brokers: Socket,
                                                kryo: KryoSerializer) {
        logger.warn("Unsupported operation, message is discarded")
    }

    override fun processBrokerForwardPingreq(message: InternalServerMessage, clients: Socket, brokers: Socket,
                                             kryo: KryoSerializer) {
        logger.warn("Unsupported operation, message is discarded")
    }

    override fun processBrokerForwardSubscribe(message: InternalServerMessage, clients: Socket, brokers: Socket,
                                               kryo: KryoSerializer) {
        logger.warn("Unsupported operation, message is discarded")
    }

    override fun processBrokerForwardUnsubscribe(message: InternalServerMessage, clients: Socket, brokers: Socket,
                                                 kryo: KryoSerializer) {
        logger.warn("Unsupported operation, message is discarded")
    }

    override fun processBrokerForwardPublish(message: InternalServerMessage, clients: Socket, brokers: Socket,
                                             kryo: KryoSerializer) {
        logger.warn("Unsupported operation, message is discarded")
    }

}
