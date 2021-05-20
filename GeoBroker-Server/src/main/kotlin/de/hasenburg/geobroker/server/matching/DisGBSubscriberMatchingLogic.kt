package de.hasenburg.geobroker.server.matching

import de.hasenburg.geobroker.commons.model.message.Payload.*
import de.hasenburg.geobroker.commons.model.message.ReasonCode
import de.hasenburg.geobroker.commons.model.message.toZMsg
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.server.communication.ZMQProcess_BrokerCommunicator
import de.hasenburg.geobroker.server.distribution.BrokerAreaManager
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper
import de.hasenburg.geobroker.server.storage.client.ClientDirectory
import kotlinx.serialization.json.Json
import org.apache.logging.log4j.LogManager
import org.zeromq.ZMQ.Socket
import org.zeromq.ZMsg

private val logger = LogManager.getLogger()

/**
 * One GeoBroker instance that does not communicate with others. Uses the [TopicAndGeofenceMapper].
 */
class DisGBAtSubscriberMatchingLogic(private val clientDirectory: ClientDirectory,
                                     private val topicAndGeofenceMapper: TopicAndGeofenceMapper,
                                     private val brokerAreaManager: BrokerAreaManager) : IMatchingLogic {

    private fun sendResponse(response: ZMsg, clients: Socket) {
        logger.trace("Sending response $response")
        response.send(clients)
    }

    override fun processCONNECT(clientIdentifier: String, payload: CONNECTPayload, clients: Socket,
                                brokers: Socket) {

        if (!weAreResponsible(clientIdentifier, payload.location, clients)) {
            return  // we are not responsible, client has been notified
        }

        val payloadResponse = connectClientAtLocalBroker(clientIdentifier, payload.location, clientDirectory, logger)
        val response = payloadResponse.toZMsg(clientIdentifier)

        sendResponse(response, clients)
    }

    override fun processDISCONNECT(clientIdentifier: String, payload: DISCONNECTPayload, clients: Socket,
                                   brokers: Socket) {

        val success = clientDirectory.removeClient(clientIdentifier)
        if (!success) {
            logger.trace("Client for {} did not exist", clientIdentifier)
            return
        }

        logger.debug("Disconnected client {}, code {}", clientIdentifier, payload.reasonCode)
        // no response to send here
    }

    override fun processPINGREQ(clientIdentifier: String, payload: PINGREQPayload, clients: Socket,
                                brokers: Socket) {

        // check whether client has moved to another broker area
        if (!weAreResponsible(clientIdentifier, payload.location, clients)) {
            return  // we are not responsible, client has been notified
        }

        val reasonCode = updateClientLocationAtLocalBroker(clientIdentifier,
                payload.location,
                clientDirectory,
                logger)

        val response = PINGRESPPayload(reasonCode).toZMsg(clientIdentifier)

        sendResponse(response, clients)
    }

    override fun processSUBSCRIBE(clientIdentifier: String, payload: SUBSCRIBEPayload, clients: Socket,
                                  brokers: Socket) {

        val reasonCode = subscribeAtLocalBroker(clientIdentifier,
                clientDirectory,
                topicAndGeofenceMapper,
                payload.topic,
                payload.geofence,
                logger)

        val response = SUBACKPayload(reasonCode).toZMsg(clientIdentifier)

        sendResponse(response, clients)
    }

    override fun processUNSUBSCRIBE(clientIdentifier: String, payload: UNSUBSCRIBEPayload, clients: Socket,
                                    brokers: Socket) {

        val reasonCode = unsubscribeAtLocalBroker(clientIdentifier,
                clientDirectory,
                topicAndGeofenceMapper,
                payload.topic,
                logger)

        val response = UNSUBACKPayload(reasonCode).toZMsg(clientIdentifier)

        sendResponse(response, clients)
    }

    override fun processPUBLISH(clientIdentifier: String, payload: PUBLISHPayload, clients: Socket,
                                brokers: Socket) {

        val reasonCode: ReasonCode
        val publisherLocation = clientDirectory.getClientLocation(clientIdentifier)

        if (publisherLocation == null) { // null if client is not connected
            logger.debug("Client {} is not connected or has not provided a location", clientIdentifier)
            reasonCode = ReasonCode.NotConnectedOrNoLocation
        } else {

            // find other brokers whose broker area intersects with the message geofence
            val otherBrokers = brokerAreaManager.getOtherBrokersIntersectingWithGeofence(payload.geofence)
            for (otherBroker in otherBrokers) {
                logger.debug("Broker area of {} intersects with message from client {}",
                        otherBroker.brokerId,
                        clientIdentifier)
                // send message to BrokerCommunicator who takes care of the rest
                BrokerForwardPublishPayload(payload, publisherLocation).toZMsg(otherBroker.brokerId).send(brokers)

            }

            var ourReasonCode = ReasonCode.NoMatchingSubscribers
            // check if own broker area intersects with the message geofence
            if (brokerAreaManager.checkOurAreaForGeofenceIntersection(payload.geofence)) {
                ourReasonCode = publishMessageToLocalClients(publisherLocation,
                        payload,
                        clientDirectory,
                        topicAndGeofenceMapper,
                        clients,
                        logger)
            }

            reasonCode = if (otherBrokers.size > 0 && ourReasonCode == ReasonCode.NoMatchingSubscribers) {
                ReasonCode.NoMatchingSubscribersButForwarded
            } else if (otherBrokers.size == 0 && ourReasonCode == ReasonCode.NoMatchingSubscribers) {
                ReasonCode.NoMatchingSubscribers
            } else {
                ReasonCode.Success
            }

        }

        // send response to publisher
        val response = PUBACKPayload(reasonCode).toZMsg(clientIdentifier)
        logger.trace("Sending response with reason code $reasonCode")
        sendResponse(response, clients)
    }

    /*****************************************************************
     * Broker Forward Methods
     ****************************************************************/

    override fun processBrokerForwardDisconnect(otherBrokerId: String, payload: BrokerForwardDisconnectPayload,
                                                clients: Socket, brokers: Socket) {
        logger.warn("Unsupported operation, message is discarded")
    }

    override fun processBrokerForwardPingreq(otherBrokerId: String, payload: BrokerForwardPingreqPayload,
                                             clients: Socket, brokers: Socket) {
        logger.warn("Unsupported operation, message is discarded")
    }

    override fun processBrokerForwardSubscribe(otherBrokerId: String, payload: BrokerForwardSubscribePayload,
                                               clients: Socket, brokers: Socket) {
        logger.warn("Unsupported operation, message is discarded")
    }

    override fun processBrokerForwardUnsubscribe(otherBrokerId: String,
                                                 payload: BrokerForwardUnsubscribePayload, clients: Socket,
                                                 brokers: Socket) {
        logger.warn("Unsupported operation, message is discarded")
    }

    /**
     * Publishes a message to local clients that originates from a client connected to another broker.
     *
     * As the other broker tells us about this message, we are responding to the other broker rather than responding
     * to the original client.
     */
    override fun processBrokerForwardPublish(otherBrokerId: String, payload: BrokerForwardPublishPayload,
                                             clients: Socket, brokers: Socket) {

        // the id is determined by ZeroMQ based on the first frame, so here it is the id of the forwarding broker
        logger.debug("Processing BrokerForwardPublish from broker {}, message is {}",
                otherBrokerId,
                payload.publishPayload.content)

        val publisherLocation = payload.publisherLocation
        val reasonCode = if (publisherLocation != null) {
            publishMessageToLocalClients(publisherLocation,
                    payload.publishPayload,
                    clientDirectory,
                    topicAndGeofenceMapper,
                    clients,
                    logger)
        } else {
            ReasonCode.ProtocolError // the publisher location was missing, so cannot match
        }

        // acknowledge publish operation to other broker, he does not expect a particular message so we just reply
        // with the response that we have generated anyways (needs to go via the clients socket as response has to
        // go out of the ZMQProcess_Server
        logger.trace("Sending response with reason code $reasonCode")
        val response = PUBACKPayload(reasonCode).toZMsg(otherBrokerId)
        sendResponse(response, clients)
    }

    /*****************************************************************
     * Message Processing Helper
     ****************************************************************/

    /**
     * Checks whether this particular broker is responsible for the client with the given location. If not, sends a
     * disconnect message and information about the responsible broker, if any exists. The client is also removed from
     * the client directory. Otherwise, does nothing.
     *
     * If [clientLocation] is null, the broker is not responsible as the location is not part of its geofence.
     *
     * @return true, if this broker is responsible, otherwise false
     */
    private fun weAreResponsible(clientIdentifier: String, clientLocation: Location?, clients: Socket): Boolean {

        if (!brokerAreaManager.checkIfOurAreaContainsLocation(clientLocation)) {
            // get responsible broker
            val repBroker = brokerAreaManager.getOtherBrokerContainingLocation(clientLocation)

            val response = DISCONNECTPayload(ReasonCode.WrongBroker, repBroker).toZMsg(clientIdentifier)
            logger.debug("Not responsible for client {}, responsible broker is {}", clientIdentifier, repBroker)

            sendResponse(response, clients)

            // TODO F: migrate client data to other broker, right now he has to update the information himself
            logger.debug("Client had {} active subscriptions",
                    clientDirectory.getCurrentClientSubscriptions(clientIdentifier))
            clientDirectory.removeClient(clientIdentifier)
            return false
        }
        return true
    }

}
