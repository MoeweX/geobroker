package de.hasenburg.geobroker.server.matching

import de.hasenburg.geobroker.commons.model.KryoSerializer
import de.hasenburg.geobroker.commons.model.message.Payload.*
import de.hasenburg.geobroker.commons.model.message.ReasonCode
import de.hasenburg.geobroker.commons.model.message.payloadToZMsg
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.server.communication.ZMQProcess_BrokerCommunicator
import de.hasenburg.geobroker.server.distribution.BrokerAreaManager
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper
import de.hasenburg.geobroker.server.storage.client.ClientDirectory
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
                                brokers: Socket, kryo: KryoSerializer) {

        if (!payload.location.isUndefined && !handleResponsibility(clientIdentifier,
                        payload.location,
                        clients,
                        kryo)) {
            return  // we are not responsible, client has been notified
        } else if (payload.location.isUndefined) {
            logger.warn("Client with an undefined location connected to broker.")
        }

        val payloadResponse = connectClientAtLocalBroker(clientIdentifier, payload.location, clientDirectory, logger)

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

    override fun processPINGREQ(clientIdentifier: String, payload: PINGREQPayload, clients: Socket,
                                brokers: Socket, kryo: KryoSerializer) {

        // check whether client has moved to another broker area
        if (!handleResponsibility(clientIdentifier, payload.location, clients, kryo)) {
            return  // we are not responsible, client has been notified
        }

        val reasonCode = updateClientLocationAtLocalBroker(clientIdentifier,
                payload.location,
                clientDirectory,
                logger)

        val response = payloadToZMsg(PINGRESPPayload(reasonCode), kryo, clientIdentifier)

        sendResponse(response, clients)
    }

    override fun processSUBSCRIBE(clientIdentifier: String, payload: SUBSCRIBEPayload, clients: Socket,
                                  brokers: Socket, kryo: KryoSerializer) {

        val reasonCode = subscribeAtLocalBroker(clientIdentifier,
                clientDirectory,
                topicAndGeofenceMapper,
                payload.topic,
                payload.geofence,
                logger)

        val response = payloadToZMsg(SUBACKPayload(reasonCode), kryo, clientIdentifier)

        sendResponse(response, clients)
    }

    override fun processUNSUBSCRIBE(clientIdentifier: String, payload: UNSUBSCRIBEPayload, clients: Socket,
                                    brokers: Socket, kryo: KryoSerializer) {

        val reasonCode = unsubscribeAtLocalBroker(clientIdentifier,
                clientDirectory,
                topicAndGeofenceMapper,
                payload.topic,
                logger)

        val response = payloadToZMsg(UNSUBACKPayload(reasonCode), kryo, clientIdentifier)

        sendResponse(response, clients)
    }

    override fun processPUBLISH(clientIdentifier: String, payload: PUBLISHPayload, clients: Socket,
                                brokers: Socket, kryo: KryoSerializer) {

        val reasonCode: ReasonCode
        val publisherLocation = clientDirectory.getClientLocation(clientIdentifier)

        if (publisherLocation == null) { // null if client is not connected
            logger.debug("Client {} is not connected", clientIdentifier)
            reasonCode = ReasonCode.NotConnected
        } else {

            // find other brokers whose broker area intersects with the message geofence
            val otherBrokers = brokerAreaManager.getOtherBrokersIntersectingWithGeofence(payload.geofence)
            for (otherBroker in otherBrokers) {
                logger.debug("Broker area of {} intersects with message from client {}",
                        otherBroker.brokerId,
                        clientIdentifier)
                // send message to BrokerCommunicator who takes care of the rest
                ZMQProcess_BrokerCommunicator.generatePULLSocketMessage(otherBroker.brokerId,
                        payloadToZMsg(BrokerForwardPublishPayload(payload, publisherLocation), kryo)).send(brokers)

            }

            var ourReasonCode = ReasonCode.NoMatchingSubscribers
            // check if own broker area intersects with the message geofence
            if (brokerAreaManager.checkOurAreaForGeofenceIntersection(payload.geofence)) {
                ourReasonCode = publishMessageToLocalClients(publisherLocation,
                        payload,
                        clientDirectory,
                        topicAndGeofenceMapper,
                        clients,
                        logger,
                        kryo)
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
        val response = payloadToZMsg(PUBACKPayload(reasonCode), kryo, clientIdentifier)
        logger.trace("Sending response with reason code $reasonCode")
        sendResponse(response, clients)
    }

    /*****************************************************************
     * Broker Forward Methods
     ****************************************************************/

    override fun processBrokerForwardDisconnect(otherBrokerId: String, payload: BrokerForwardDisconnectPayload,
                                                clients: Socket, brokers: Socket, kryo: KryoSerializer) {
        logger.warn("Unsupported operation, message is discarded")
    }

    override fun processBrokerForwardPingreq(otherBrokerId: String, payload: BrokerForwardPingreqPayload,
                                             clients: Socket, brokers: Socket, kryo: KryoSerializer) {
        logger.warn("Unsupported operation, message is discarded")
    }

    override fun processBrokerForwardSubscribe(otherBrokerId: String, payload: BrokerForwardSubscribePayload,
                                               clients: Socket, brokers: Socket, kryo: KryoSerializer) {
        logger.warn("Unsupported operation, message is discarded")
    }

    override fun processBrokerForwardUnsubscribe(otherBrokerId: String,
                                                 payload: BrokerForwardUnsubscribePayload, clients: Socket,
                                                 brokers: Socket, kryo: KryoSerializer) {
        logger.warn("Unsupported operation, message is discarded")
    }

    /**
     * Publishes a message to local clients that originates from a client connected to another broker.
     *
     * As the other broker tells us about this message, we are responding to the other broker rather than responding
     * to the original client.
     */
    override fun processBrokerForwardPublish(otherBrokerId: String, payload: BrokerForwardPublishPayload,
                                             clients: Socket, brokers: Socket, kryo: KryoSerializer) {

        // the id is determined by ZeroMQ based on the first frame, so here it is the id of the forwarding broker
        logger.debug("Processing BrokerForwardPublish from broker {}, message is {}",
                otherBrokerId,
                payload.publishPayload.content)

        val reasonCode = publishMessageToLocalClients(payload.publisherLocation,
                payload.publishPayload,
                clientDirectory,
                topicAndGeofenceMapper,
                clients,
                logger,
                kryo)


        // acknowledge publish operation to other broker, he does not expect a particular message so we just reply
        // with the response that we have generated anyways (needs to go via the clients socket as response has to
        // go out of the ZMQProcess_Server
        logger.trace("Sending response with reason code $reasonCode")
        val response = payloadToZMsg(PUBACKPayload(reasonCode), kryo, otherBrokerId)
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
     * @return true, if this broker is responsible, otherwise false
     */
    private fun handleResponsibility(clientIdentifier: String, clientLocation: Location, clients: Socket,
                                     kryo: KryoSerializer): Boolean {
        if (!brokerAreaManager.checkIfOurAreaContainsLocation(clientLocation)) {
            // get responsible broker
            val repBroker = brokerAreaManager.getOtherBrokerContainingLocation(clientLocation)

            val response = payloadToZMsg(DISCONNECTPayload(ReasonCode.WrongBroker, repBroker), kryo, clientIdentifier)
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
