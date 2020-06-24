package de.hasenburg.geobroker.server.matching

import de.hasenburg.geobroker.commons.model.message.Payload.*
import de.hasenburg.geobroker.commons.model.message.ReasonCode
import de.hasenburg.geobroker.commons.model.message.toZMsg
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper
import de.hasenburg.geobroker.server.storage.client.ClientDirectory
import kotlinx.serialization.json.Json
import org.apache.logging.log4j.LogManager
import org.zeromq.ZMQ.Socket
import org.zeromq.ZMsg

private val logger = LogManager.getLogger()

/**
 * One GeoBroker instance that does not communicate with others. Uses the [de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper].
 */
class SingleGeoBrokerMatchingLogic(private val clientDirectory: ClientDirectory,
                                   private val topicAndGeofenceMapper: TopicAndGeofenceMapper) : IMatchingLogic {

    private fun sendResponse(response: ZMsg, clients: Socket) {
        logger.trace("Sending response $response")
        response.send(clients)
    }

    override fun processCONNECT(clientIdentifier: String, payload: CONNECTPayload, clients: Socket, json: Json) {
        val payloadResponse = connectClientAtLocalBroker(clientIdentifier, payload.location, clientDirectory, logger)
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
        val reasonCode = subscribeAtLocalBroker(clientIdentifier,
                clientDirectory,
                topicAndGeofenceMapper,
                payload.topic,
                payload.geofence,
                logger)

        val response = SUBACKPayload(reasonCode).toZMsg(json, clientIdentifier)

        sendResponse(response, clients)
    }

    override fun processUNSUBSCRIBE(clientIdentifier: String, payload: UNSUBSCRIBEPayload, clients: Socket, json: Json) {
        val reasonCode = unsubscribeAtLocalBroker(clientIdentifier,
                clientDirectory,
                topicAndGeofenceMapper,
                payload.topic,
                logger)

        val response = UNSUBACKPayload(reasonCode).toZMsg(json, clientIdentifier)

        sendResponse(response, clients)
    }

    override fun processPUBLISH(clientIdentifier: String, payload: PUBLISHPayload, clients: Socket, json: Json) {

        val reasonCode: ReasonCode
        val publisherLocation = clientDirectory.getClientLocation(clientIdentifier)

        reasonCode = if (publisherLocation == null) { // null if client is not connected
            logger.debug("Client {} is not connected", clientIdentifier)
            ReasonCode.NotConnectedOrNoLocation
        } else {
            publishMessageToLocalClients(publisherLocation,
                    payload,
                    clientDirectory,
                    topicAndGeofenceMapper,
                    clients,
                    logger,
                    json)
        }

        // send response to publisher
        val response = PUBACKPayload(reasonCode).toZMsg(json, clientIdentifier)
        sendResponse(response, clients)
    }
}
