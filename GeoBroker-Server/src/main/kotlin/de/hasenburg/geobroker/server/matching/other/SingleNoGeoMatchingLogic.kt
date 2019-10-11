package de.hasenburg.geobroker.server.matching.other

import de.hasenburg.geobroker.commons.model.KryoSerializer
import de.hasenburg.geobroker.commons.model.message.Payload.*
import de.hasenburg.geobroker.commons.model.message.ReasonCode
import de.hasenburg.geobroker.commons.model.message.payloadToZMsg
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.server.matching.*
import de.hasenburg.geobroker.server.storage.client.ClientDirectory
import de.hasenburg.geobroker.server.storage.other.nogeo.NoGeoSubscriptionIndexingStructure
import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.logging.log4j.LogManager
import org.zeromq.ZMQ.Socket
import org.zeromq.ZMsg

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

    private fun sendResponse(response: ZMsg, clients: Socket) {
        logger.trace("Sending response $response")
        response.send(clients)
    }

    override fun processCONNECT(clientIdentifier: String, payload: CONNECTPayload, clients: Socket, brokers: Socket,
                                kryo: KryoSerializer) {

        val payloadResponse = connectClientAtLocalBroker(clientIdentifier, payload.location, clientDirectory,
                logger)

        val response = payloadToZMsg(payloadResponse, kryo, clientIdentifier)

        sendResponse(response, clients)
    }

    override fun processDISCONNECT(clientIdentifier: String, payload: DISCONNECTPayload, clients: Socket,
                                   brokers: Socket, kryo: KryoSerializer) {

        val success = clientDirectory.removeClient(clientIdentifier)
        if (!success) {
            logger.trace("Client for {} did not exist", clientIdentifier)
            return
        }

        logger.debug("Disconnected client {}, code {}", clientIdentifier, payload.reasonCode)
    }

    override fun processPINGREQ(clientIdentifier: String, payload: PINGREQPayload, clients: Socket, brokers: Socket,
                                kryo: KryoSerializer) {

        val reasonCode = updateClientLocationAtLocalBroker(clientIdentifier,
                payload.location,
                clientDirectory,
                logger)

        val response = payloadToZMsg(PINGRESPPayload(reasonCode), kryo, clientIdentifier)

        sendResponse(response, clients)
    }

    override fun processSUBSCRIBE(clientIdentifier: String, payload: SUBSCRIBEPayload, clients: Socket, brokers: Socket,
                                  kryo: KryoSerializer) {
        val reasonCode: ReasonCode

        val subscribed: ImmutablePair<ImmutablePair<String, Int>, Geofence>? =
                clientDirectory.checkIfSubscribed(clientIdentifier, payload.topic, ignore)

        if (subscribed != null) {
            // if already subscribed -> done
            reasonCode = ReasonCode.Success
        } else {
            // create subscription
            val subscriptionId = clientDirectory.updateSubscription(clientIdentifier, payload.topic, ignore)
            // index subscription
            reasonCode = if (subscriptionId == null) {
                logger.debug("Client {} is not connected", clientIdentifier)
                ReasonCode.NotConnected
            } else {
                subscriptionIndexingStructure.putSubscriptionId(subscriptionId, payload.topic)
                logger.debug("Client {} subscribed to topic {}", clientIdentifier, payload.topic)
                ReasonCode.GrantedQoS0
            }
        }

        val response = payloadToZMsg(SUBACKPayload(reasonCode), kryo, clientIdentifier)

        sendResponse(response, clients)
    }

    override fun processUNSUBSCRIBE(clientIdentifier: String, payload: UNSUBSCRIBEPayload, clients: Socket,
                                    brokers: Socket, kryo: KryoSerializer) {

        val reasonCode: ReasonCode

        // unsubscribe from client directory -> get subscription id
        val s = clientDirectory.removeSubscription(clientIdentifier, payload.topic)

        // remove from storage if existed
        reasonCode = if (s != null) {
            subscriptionIndexingStructure.removeSubscriptionId(s.subscriptionId, s.topic)
            logger.debug("Client $clientIdentifier unsubscribed from topic $payload.topic, " +
                         "subscription had the id ${s.subscriptionId}")
            ReasonCode.Success
        } else {
            logger.debug("Client $clientIdentifier has no subscription with topic $payload.topic, " +
                    "thus unable to unsubscribe")
            ReasonCode.NoSubscriptionExisted
        }

        val response = payloadToZMsg(UNSUBACKPayload(reasonCode), kryo, clientIdentifier)
        sendResponse(response, clients)
    }

    override fun processPUBLISH(clientIdentifier: String, payload: PUBLISHPayload, clients: Socket, brokers: Socket,
                                kryo: KryoSerializer) {
        val reasonCode: ReasonCode
        val publisherLocation = clientDirectory.getClientLocation(clientIdentifier)

        if (publisherLocation == null) { // null if client is not connected
            logger.debug("Client {} is not connected", clientIdentifier)
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
                val toPublish = payloadToZMsg(payload, kryo, subscriberClientIdentifier)
                logger.trace("Publishing $toPublish")
                toPublish.send(clients)
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
        val response = payloadToZMsg(PUBACKPayload(reasonCode), kryo, clientIdentifier)
        sendResponse(response, clients)
    }



    /*****************************************************************
     * Broker Forward Methods
     ****************************************************************/

    override fun processBrokerForwardDisconnect(otherBrokerId: String, payload: BrokerForwardDisconnectPayload,
                                                clients: Socket, brokers: Socket,
                                                kryo: KryoSerializer) {
        logger.warn("Unsupported operation, message is discarded")
    }

    override fun processBrokerForwardPingreq(otherBrokerId: String, payload: BrokerForwardPingreqPayload,
                                             clients: Socket, brokers: Socket,
                                             kryo: KryoSerializer) {
        logger.warn("Unsupported operation, message is discarded")
    }

    override fun processBrokerForwardSubscribe(otherBrokerId: String, payload: BrokerForwardSubscribePayload,
                                               clients: Socket, brokers: Socket,
                                               kryo: KryoSerializer) {
        logger.warn("Unsupported operation, message is discarded")
    }

    override fun processBrokerForwardUnsubscribe(otherBrokerId: String, payload: BrokerForwardUnsubscribePayload,
                                                 clients: Socket, brokers: Socket,
                                                 kryo: KryoSerializer) {
        logger.warn("Unsupported operation, message is discarded")
    }

    override fun processBrokerForwardPublish(otherBrokerId: String, payload: BrokerForwardPublishPayload,
                                             clients: Socket, brokers: Socket,
                                             kryo: KryoSerializer) {
        logger.warn("Unsupported operation, message is discarded")
    }

}
