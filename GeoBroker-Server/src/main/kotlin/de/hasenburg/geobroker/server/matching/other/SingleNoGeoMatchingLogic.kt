package de.hasenburg.geobroker.server.matching.other

import de.hasenburg.geobroker.commons.model.message.Payload.*
import de.hasenburg.geobroker.commons.model.message.ReasonCode
import de.hasenburg.geobroker.commons.model.message.toZMsg
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.server.matching.*
import de.hasenburg.geobroker.server.storage.client.ClientDirectory
import de.hasenburg.geobroker.server.storage.other.nogeo.NoGeoSubscriptionIndexingStructure
import kotlinx.serialization.json.Json
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

    override fun processCONNECT(clientIdentifier: String, payload: CONNECTPayload, clients: Socket, json: Json) {

        val payloadResponse = connectClientAtLocalBroker(clientIdentifier, payload.location, clientDirectory,
                logger)
        val response = payloadResponse.toZMsg(json, clientIdentifier)

        sendResponse(response, clients)
    }

    override fun processDISCONNECT(clientIdentifier: String, payload: DISCONNECTPayload, clients: Socket, json: Json) {

        val success = clientDirectory.removeClient(clientIdentifier)
        if (!success) {
            logger.trace("Client for {} did not exist", clientIdentifier)
            return
        }

        logger.debug("Disconnected client {}, code {}", clientIdentifier, payload.reasonCode)
        // no response to send here
    }

    override fun processPINGREQ(clientIdentifier: String, payload: PINGREQPayload, clients: Socket, json: Json) {

        val reasonCode = updateClientLocationAtLocalBroker(clientIdentifier,
                payload.location,
                clientDirectory,
                logger)

        val response = PINGRESPPayload(reasonCode).toZMsg(json, clientIdentifier)

        sendResponse(response, clients)
    }

    override fun processSUBSCRIBE(clientIdentifier: String, payload: SUBSCRIBEPayload, clients: Socket, json: Json) {
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
                ReasonCode.NotConnectedOrNoLocation
            } else {
                subscriptionIndexingStructure.putSubscriptionId(subscriptionId, payload.topic)
                logger.debug("Client {} subscribed to topic {}", clientIdentifier, payload.topic)
                ReasonCode.GrantedQoS0
            }
        }

        val response = SUBACKPayload(reasonCode).toZMsg(json, clientIdentifier)

        sendResponse(response, clients)
    }

    override fun processUNSUBSCRIBE(clientIdentifier: String, payload: UNSUBSCRIBEPayload, clients: Socket, json: Json) {

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

        val response = UNSUBACKPayload(reasonCode).toZMsg(json, clientIdentifier)
        sendResponse(response, clients)
    }

    override fun processPUBLISH(clientIdentifier: String, payload: PUBLISHPayload, clients: Socket, json: Json) {
        val reasonCode: ReasonCode
        val publisherLocation = clientDirectory.getClientLocation(clientIdentifier)

        if (publisherLocation == null) { // null if client is not connected
            logger.debug("Client {} is not connected or has not provided a location", clientIdentifier)
            reasonCode = ReasonCode.NotConnectedOrNoLocation
        } else {
            logger.debug("Publishing topic {} to all subscribers", payload.topic)

            // get subscriptions that match the topic
            val subscriptionIds =
                    subscriptionIndexingStructure.getSubscriptionIds(payload.topic)

            // publish message to subscribers
            for (subscriptionId in subscriptionIds) {
                val subscriberClientIdentifier = subscriptionId.left
                logger.debug("Client {} is a subscriber", subscriberClientIdentifier)
                val toPublish = payload.toZMsg(json, subscriberClientIdentifier)
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
        val response = PUBACKPayload(reasonCode).toZMsg(json, clientIdentifier)
        sendResponse(response, clients)
    }

}
