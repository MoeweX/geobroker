package de.hasenburg.geobroker.server.matching

import de.hasenburg.geobroker.commons.model.message.ControlPacketType
import de.hasenburg.geobroker.commons.model.KryoSerializer
import de.hasenburg.geobroker.commons.model.message.ReasonCode
import de.hasenburg.geobroker.commons.model.message.payloads.*
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

    override fun processCONNECT(message: InternalServerMessage, clients: Socket, brokers: Socket, kryo: KryoSerializer) {
        val payload = message.payload.connectPayload.get()

        if (!handleResponsibility(message.clientIdentifier, payload.location, clients, kryo)) {
            return  // we are not responsible, client has been notified
        }

        val response = connectClientAtLocalBroker(message.clientIdentifier, payload.location, clientDirectory,
                logger, kryo)

        logger.trace("Sending response $response")
        response.getZMsg(kryo).send(clients)
    }

    override fun processDISCONNECT(message: InternalServerMessage, clients: Socket, brokers: Socket, kryo: KryoSerializer) {
        val payload = message.payload.disconnectPayload.get()

        val success = clientDirectory.removeClient(message.clientIdentifier)
        if (!success) {
            logger.trace("Client for {} did not exist", message.clientIdentifier)
            return
        }

        logger.debug("Disconnected client {}, code {}", message.clientIdentifier, payload.reasonCode)
    }

    override fun processPINGREQ(message: InternalServerMessage, clients: Socket, brokers: Socket, kryo: KryoSerializer) {
        val payload = message.payload.pingreqPayload.get()

        // check whether client has moved to another broker area
        if (!handleResponsibility(message.clientIdentifier, payload.location, clients, kryo)) {
            return  // we are not responsible, client has been notified
        }

        val reasonCode = updateClientLocationAtLocalBroker(message.clientIdentifier,
                payload.location,
                clientDirectory,
                logger,
                kryo)

        val response = InternalServerMessage(message.clientIdentifier,
                ControlPacketType.PINGRESP,
                PINGRESPPayload(reasonCode))

        logger.trace("Sending response $response")
        response.getZMsg(kryo).send(clients)
    }

    override fun processSUBSCRIBE(message: InternalServerMessage, clients: Socket, brokers: Socket, kryo: KryoSerializer) {
        val payload = message.payload.subscribePayload.get()

        val reasonCode = subscribeAtLocalBroker(message.clientIdentifier,
                clientDirectory,
                topicAndGeofenceMapper,
                payload.topic,
                payload.geofence,
                logger,
                kryo)

        val response = InternalServerMessage(message.clientIdentifier,
                ControlPacketType.SUBACK,
                SUBACKPayload(reasonCode))

        logger.trace("Sending response $response")
        response.getZMsg(kryo).send(clients)
    }

    override fun processUNSUBSCRIBE(message: InternalServerMessage, clients: Socket, brokers: Socket, kryo: KryoSerializer) {
        val payload = message.payload.unsubscribePayload.get()

        val reasonCode = unsubscribeAtLocalBroker(message.clientIdentifier,
                clientDirectory,
                topicAndGeofenceMapper,
                payload.topic,
                logger,
                kryo)

        val response = InternalServerMessage(message.clientIdentifier,
                ControlPacketType.UNSUBACK,
                UNSUBACKPayload(reasonCode))

        logger.trace("Sending response $response")
        response.getZMsg(kryo).send(clients)
    }

    override fun processPUBLISH(message: InternalServerMessage, clients: Socket, brokers: Socket, kryo: KryoSerializer) {
        val reasonCode: ReasonCode
        val payload = message.payload.publishPayload.get()
        val publisherLocation = clientDirectory.getClientLocation(message.clientIdentifier)

        if (publisherLocation == null) { // null if client is not connected
            logger.debug("Client {} is not connected", message.clientIdentifier)
            reasonCode = ReasonCode.NotConnected
        } else {

            // find other brokers whose broker area intersects with the message geofence
            val otherBrokers = brokerAreaManager.getOtherBrokersIntersectingWithGeofence(payload.geofence)
            for (otherBroker in otherBrokers) {
                logger.debug("Broker area of {} intersects with message from client {}",
                        otherBroker.brokerId,
                        message.clientIdentifier)
                // send message to BrokerCommunicator who takes care of the rest
                ZMQProcess_BrokerCommunicator.generatePULLSocketMessage(otherBroker.brokerId,
                        InternalBrokerMessage(ControlPacketType.BrokerForwardPublish,
                                BrokerForwardPublishPayload(payload, publisherLocation)), kryo).send(brokers)

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
        response.getZMsg(kryo).send(clients)

    }

    /*****************************************************************
     * Broker Forward Methods
     ****************************************************************/

    override fun processBrokerForwardDisconnect(message: InternalServerMessage, clients: Socket, brokers: Socket, kryo: KryoSerializer) {
        logger.warn("Unsupported operation, message is discarded")
    }

    override fun processBrokerForwardPingreq(message: InternalServerMessage, clients: Socket, brokers: Socket, kryo: KryoSerializer) {
        logger.warn("Unsupported operation, message is discarded")
    }

    override fun processBrokerForwardSubscribe(message: InternalServerMessage, clients: Socket, brokers: Socket, kryo: KryoSerializer) {
        logger.warn("Unsupported operation, message is discarded")
    }

    override fun processBrokerForwardUnsubscribe(message: InternalServerMessage, clients: Socket, brokers: Socket, kryo: KryoSerializer) {
        logger.warn("Unsupported operation, message is discarded")
    }

    /**
     * Publishes a message to local clients that originates from a client connected to another broker.
     *
     * As the other broker tells us about this message, we are responding to the other broker rather than responding
     * to the original client.
     */
    override fun processBrokerForwardPublish(message: InternalServerMessage, clients: Socket, brokers: Socket, kryo: KryoSerializer) {
        val payload = message.payload.brokerForwardPublishPayload.get()

        // the id is determined by ZeroMQ based on the first frame, so here it is the id of the forwarding broker
        val otherBrokerId = message.clientIdentifier
        logger.trace("Processing BrokerForwardPublish from broker {}", otherBrokerId)

        val reasonCode = publishMessageToLocalClients(payload.publisherLocation,
                payload.getPublishPayload(),
                clientDirectory,
                topicAndGeofenceMapper,
                clients,
                logger,
                kryo)

        val response = InternalServerMessage(otherBrokerId, ControlPacketType.PUBACK, PUBACKPayload(reasonCode))

        // acknowledge publish operation to other broker, he does not expect a particular message so we just reply
        // with the response that we have generated anyways (needs to go via the clients socket as response has to
        // go out of the ZMQProcess_Server
        logger.trace("Sending response with reason code $reasonCode")
        response.getZMsg(kryo).send(clients)
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
    private fun handleResponsibility(clientIdentifier: String, clientLocation: Location, clients: Socket, kryo: KryoSerializer): Boolean {
        if (!brokerAreaManager.checkIfOurAreaContainsLocation(clientLocation)) {
            // get responsible broker
            val repBroker = brokerAreaManager.getOtherBrokerContainingLocation(clientLocation)

            val response = InternalServerMessage(clientIdentifier,
                    ControlPacketType.DISCONNECT,
                    DISCONNECTPayload(ReasonCode.WrongBroker, repBroker))
            logger.debug("Not responsible for client {}, responsible broker is {}", clientIdentifier, repBroker)

            response.getZMsg(kryo).send(clients)

            // TODO F: migrate client data to other broker, right now he has to update the information himself
            logger.debug("Client had {} active subscriptions",
                    clientDirectory.getCurrentClientSubscriptions(clientIdentifier))
            clientDirectory.removeClient(clientIdentifier)
            return false
        }
        return true
    }

}
