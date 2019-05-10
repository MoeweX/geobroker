package de.hasenburg.geobroker.server.matching

import de.hasenburg.geobroker.commons.model.message.ControlPacketType
import de.hasenburg.geobroker.commons.model.message.ReasonCode
import de.hasenburg.geobroker.commons.model.message.payloads.BrokerForwardPublishPayload
import de.hasenburg.geobroker.commons.model.message.payloads.DISCONNECTPayload
import de.hasenburg.geobroker.commons.model.message.payloads.PUBACKPayload
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.server.communication.InternalBrokerMessage
import de.hasenburg.geobroker.server.communication.InternalServerMessage
import de.hasenburg.geobroker.server.communication.ZMQProcess_BrokerCommunicator
import de.hasenburg.geobroker.server.distribution.BrokerAreaManager
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper
import de.hasenburg.geobroker.server.storage.client.ClientDirectory
import org.apache.logging.log4j.LogManager
import org.zeromq.ZMQ.Socket

private val logger = LogManager.getLogger()

/**
 * One GeoBroker instance that does not communicate with others. Uses the [TopicAndGeofenceMapper].
 */
class DisGBAtSubscriberMatchingLogic(private val clientDirectory: ClientDirectory,
                                     private val topicAndGeofenceMapper: TopicAndGeofenceMapper,
                                     private val brokerAreaManager: BrokerAreaManager) : IMatchingLogic {

    override fun processCONNECT(message: InternalServerMessage, clients: Socket, brokers: Socket) {
        val payload = message.payload.connectPayload.get()

        if (!handleResponsibility(message.clientIdentifier, payload.location, clients)) {
            return  // we are not responsible, client has been notified
        }

        val response = CommonMatchingTasks.connectClientAtLocalBroker(message.clientIdentifier,
                payload.location,
                clientDirectory,
                logger)

        logger.trace("Sending response $response")
        response.zMsg.send(clients)
    }

    override fun processDISCONNECT(message: InternalServerMessage, clients: Socket, brokers: Socket) {
        val payload = message.payload.disconnectPayload.get()

        val success = clientDirectory.removeClient(message.clientIdentifier)
        if (!success) {
            logger.trace("Client for {} did not exist", message.clientIdentifier)
            return
        }

        logger.debug("Disconnected client {}, code {}", message.clientIdentifier, payload.reasonCode)

    }

    override fun processPINGREQ(message: InternalServerMessage, clients: Socket, brokers: Socket) {
        val payload = message.payload.pingreqPayload.get()

        // check whether client has moved to another broker area
        if (!handleResponsibility(message.clientIdentifier, payload.location, clients)) {
            return  // we are not responsible, client has been notified
        }

        val response = CommonMatchingTasks.updateClientLocationAtLocalBroker(message.clientIdentifier,
                payload.location,
                clientDirectory,
                logger)

        logger.trace("Sending response $response")
        response.zMsg.send(clients)
    }

    override fun processSUBSCRIBE(message: InternalServerMessage, clients: Socket, brokers: Socket) {
        val payload = message.payload.subscribePayload.get()

        val response = CommonMatchingTasks.subscribeAtLocalBroker(message.clientIdentifier,
                clientDirectory,
                topicAndGeofenceMapper,
                payload.topic,
                payload.geofence,
                logger)

        logger.trace("Sending response $response")
        response.zMsg.send(clients)
    }

    override fun processUNSUBSCRIBE(message: InternalServerMessage, clients: Socket, brokers: Socket) {
        // TODO Implement
        throw RuntimeException("Not yet implemented")
    }

    override fun processPUBLISH(message: InternalServerMessage, clients: Socket, brokers: Socket) {
        val reasonCode: ReasonCode
        val payload = message.payload.publishPayload.get()
        val publisherLocation = clientDirectory.getClientLocation(message.clientIdentifier)

        if (publisherLocation == null) { // null if client is not connected
            logger.debug("Client {} is not connected", message.clientIdentifier)
            reasonCode = ReasonCode.NotConnected
        } else {

            // find other brokers whose broker area intersects with the message geofence
            val otherBrokers = brokerAreaManager.getOtherBrokersForMessageGeofence(payload.geofence)
            for (otherBroker in otherBrokers) {
                logger.trace("Broker area of {} intersects with message from client {}",
                        otherBroker.brokerId,
                        message.clientIdentifier)
                // send message to BrokerCommunicator who takes care of the rest
                ZMQProcess_BrokerCommunicator.generatePULLSocketMessage(otherBroker.brokerId,
                        InternalBrokerMessage(ControlPacketType.BrokerForwardPublish,
                                BrokerForwardPublishPayload(payload, publisherLocation))).send(brokers)

            }


            var ourReasonCode = ReasonCode.NoMatchingSubscribers
            // check if own broker area intersects with the message geofence
            if (brokerAreaManager.checkOurAreaForMessageGeofence(payload.geofence)) {
                ourReasonCode = CommonMatchingTasks.publishMessageToLocalClients(publisherLocation,
                        payload,
                        clientDirectory,
                        topicAndGeofenceMapper,
                        clients,
                        logger)
            }

            if (otherBrokers.size > 0 && ourReasonCode == ReasonCode.NoMatchingSubscribers) {
                reasonCode = ReasonCode.NoMatchingSubscribersButForwarded
            } else if (otherBrokers.size == 0 && ourReasonCode == ReasonCode.NoMatchingSubscribers) {
                reasonCode = ReasonCode.NoMatchingSubscribers
            } else {
                reasonCode = ReasonCode.Success
            }

        }

        val response = InternalServerMessage(message.clientIdentifier,
                ControlPacketType.PUBACK,
                PUBACKPayload(reasonCode))

        // send response to publisher
        logger.trace("Sending response with reason code $reasonCode")
        response.zMsg.send(clients)

    }

    override fun processBrokerForwardPublish(message: InternalServerMessage, clients: Socket, brokers: Socket) {
        // we received this because another broker knows that our area intersects and he knows the publishing client is connected
        val payload = message.payload.brokerForwardPublishPayload.get()

        // the id is determined by ZeroMQ based on the first frame, so here it is the id of the forwarding broker
        val otherBrokerId = message.clientIdentifier
        logger.trace("Processing BrokerForwardPublish from broker {}", otherBrokerId)

        val reasonCode = CommonMatchingTasks.publishMessageToLocalClients(payload.publisherLocation,
                payload.getPublishPayload(),
                clientDirectory,
                topicAndGeofenceMapper,
                clients,
                logger)

        val response = InternalServerMessage(otherBrokerId,
                ControlPacketType.PUBACK,
                PUBACKPayload(reasonCode))

        // acknowledge publish operation to other broker, he does not expect a particular message so we just reply
        // with the response that we have generated anyways (needs to go via the clients socket as response has to
        // go out of the ZMQProcess_Server
        logger.trace("Sending response with reason code $reasonCode")
        response.zMsg.send(clients)
    }

    /*****************************************************************
     * Message Processing Helper
     */

    /**
     * Checks whether this particular broker is responsible for the client with the given location. If not, sends a
     * disconnect message and information about the responsible broker, if any exists. The client is also removed from
     * the client directory. Otherwise, does nothing.
     *
     * @return true, if this broker is responsible, otherwise false
     */
    private fun handleResponsibility(clientIdentifier: String, clientLocation: Location, clients: Socket): Boolean {
        if (!brokerAreaManager.checkIfResponsibleForClientLocation(clientLocation)) {
            // get responsible broker
            val repBroker = brokerAreaManager.getOtherBrokerForClientLocation(clientLocation)

            val response = InternalServerMessage(clientIdentifier,
                    ControlPacketType.DISCONNECT,
                    DISCONNECTPayload(ReasonCode.WrongBroker, repBroker))
            logger.debug("Not responsible for client {}, responsible broker is {}", clientIdentifier, repBroker)

            response.zMsg.send(clients)

            // TODO F: migrate client data to other broker, right now he has to update the information himself
            logger.debug("Client had {} active subscriptions",
                    clientDirectory.getCurrentClientSubscriptions(clientIdentifier))
            clientDirectory.removeClient(clientIdentifier)
            return false
        }
        return true
    }

}
