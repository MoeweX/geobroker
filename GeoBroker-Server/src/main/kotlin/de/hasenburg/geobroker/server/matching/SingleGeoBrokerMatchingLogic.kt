package de.hasenburg.geobroker.server.matching

import de.hasenburg.geobroker.commons.model.message.ControlPacketType
import de.hasenburg.geobroker.commons.model.message.ReasonCode
import de.hasenburg.geobroker.commons.model.message.payloads.PUBACKPayload
import de.hasenburg.geobroker.server.communication.InternalServerMessage
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper
import de.hasenburg.geobroker.server.storage.client.ClientDirectory
import org.apache.logging.log4j.LogManager
import org.zeromq.ZMQ.Socket

private val logger = LogManager.getLogger()

/**
 * One GeoBroker instance that does not communicate with others. Uses the [de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper].
 */
class SingleGeoBrokerMatchingLogic(private val clientDirectory: ClientDirectory,
                                   private val topicAndGeofenceMapper: TopicAndGeofenceMapper) : IMatchingLogic {

    override fun processCONNECT(message: InternalServerMessage, clients: Socket, brokers: Socket) {
        val payload = message.payload.connectPayload.get()

        val response = connectClientAtLocalBroker(message.clientIdentifier,
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

        val response = updateClientLocationAtLocalBroker(message.clientIdentifier,
                payload.location,
                clientDirectory,
                logger)

        logger.trace("Sending response $response")
        response.zMsg.send(clients)
    }

    override fun processSUBSCRIBE(message: InternalServerMessage, clients: Socket, brokers: Socket) {
        val payload = message.payload.subscribePayload.get()

        val response = subscribeAtLocalBroker(message.clientIdentifier,
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
            reasonCode = publishMessageToLocalClients(publisherLocation,
                    payload,
                    clientDirectory,
                    topicAndGeofenceMapper,
                    clients,
                    logger)
        }

        // send response to publisher
        logger.trace("Sending response with reason code $reasonCode")
        val response = InternalServerMessage(message.clientIdentifier,
                ControlPacketType.PUBACK,
                PUBACKPayload(reasonCode))
        response.zMsg.send(clients)
    }

    override fun processBrokerForwardPublish(message: InternalServerMessage, clients: Socket, brokers: Socket) {
        logger.warn("Unsupported operation, message is discarded")
    }

}
