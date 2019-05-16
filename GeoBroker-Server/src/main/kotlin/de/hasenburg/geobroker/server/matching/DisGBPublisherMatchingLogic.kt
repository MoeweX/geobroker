package de.hasenburg.geobroker.server.matching

import de.hasenburg.geobroker.client.communication.InternalClientMessage
import de.hasenburg.geobroker.commons.model.message.ControlPacketType
import de.hasenburg.geobroker.commons.model.message.ReasonCode
import de.hasenburg.geobroker.commons.model.message.payloads.*
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.server.communication.InternalBrokerMessage
import de.hasenburg.geobroker.server.communication.InternalServerMessage
import de.hasenburg.geobroker.server.communication.ZMQProcess_BrokerCommunicator
import de.hasenburg.geobroker.server.distribution.BrokerAreaManager
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper
import de.hasenburg.geobroker.server.storage.client.ClientDirectory
import de.hasenburg.geobroker.server.storage.client.SubscriptionAffection
import org.apache.logging.log4j.LogManager
import org.zeromq.ZMQ

private val logger = LogManager.getLogger()

class DisGBAtPublisherMatchingLogic constructor(private val clientDirectory: ClientDirectory,
                                                private val topicAndGeofenceMapper: TopicAndGeofenceMapper,
                                                private val brokerAreaManager: BrokerAreaManager) : IMatchingLogic {

    private val subscriptionAffection = SubscriptionAffection()

    override fun processCONNECT(message: InternalServerMessage, clients: ZMQ.Socket, brokers: ZMQ.Socket) {
        val payload = message.payload.connectPayload.get()

        if (!handleResponsibility(message.clientIdentifier, payload.location, clients)) {
            return  // we are not responsible, client has been notified
        }

        val response = connectClientAtLocalBroker(message.clientIdentifier, payload.location, clientDirectory, logger)

        logger.trace("Sending response $response")
        response.zMsg.send(clients)
    }

    override fun processDISCONNECT(message: InternalServerMessage, clients: ZMQ.Socket, brokers: ZMQ.Socket) {
        val payload = message.payload.disconnectPayload.get()

        val success = clientDirectory.removeClient(message.clientIdentifier)
        if (!success) {
            logger.trace("Client for {} did not exist", message.clientIdentifier)
            return
        }

        logger.debug("Disconnected client {}, code {}", message.clientIdentifier, payload.reasonCode)
    }

    override fun processPINGREQ(message: InternalServerMessage, clients: ZMQ.Socket, brokers: ZMQ.Socket) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun processSUBSCRIBE(message: InternalServerMessage, clients: ZMQ.Socket, brokers: ZMQ.Socket) {
        val payload = message.payload.subscribePayload.get()

        /*****************************************************************
         * Local Things
         * - done first to create subscription if did not exist
         * - we always create a subscription, even if not in area, so that the "main broker" knows about any
         * - to simplify code, the subscription id is also added to raster entries outside of the broker area as this
         * does not affect the result (see [processBrokerForwardSubscribe] for a longer explanation)
         ****************************************************************/

        val reasonCode = subscribeAtLocalBroker(message.clientIdentifier,
                clientDirectory,
                topicAndGeofenceMapper,
                payload.topic,
                payload.geofence,
                logger)
        val subscriptionId = clientDirectory.getSubscription(message.clientIdentifier, payload.topic)?.subscriptionId

        // it is possible that someone has already unsubscribed again
        if (subscriptionId == null) {
            logger.warn("Subscription was deleted again before it could be send to other brokers")
            return
        }

        /*****************************************************************
         * Remote Things
         * ****************************************************************/

        // calculate what brokers are affected by the subscription's geofence
        val otherAffectedBrokers = brokerAreaManager.getOtherBrokersIntersectingWithGeofence(payload.geofence)

        // forward message to all now affected brokers
        for (otherAffectedBroker in otherAffectedBrokers) {
            logger.trace("""|Broker area of ${otherAffectedBroker.brokerId} intersects with subscription to topic
                            |${payload.topic}} from client ${message.clientIdentifier}""".trimMargin())
            // send message to BrokerCommunicator who takes care of the rest
            ZMQProcess_BrokerCommunicator.generatePULLSocketMessage(otherAffectedBroker.brokerId,
                    InternalBrokerMessage(ControlPacketType.BrokerForwardSubscribe,
                            BrokerForwardSubscribePayload(message.clientIdentifier, payload))).send(brokers)
        }

        // update broker affection -> returns now not anymore affected brokers
        val notAnymoreAffectedOtherBrokers = subscriptionAffection.updateAffections(subscriptionId,
                otherAffectedBrokers)

        // unsubscribe these now not anymore affected brokers
        for (notAnymoreAffectedOtherBroker in notAnymoreAffectedOtherBrokers) {
            logger.trace("""|Broker area of ${notAnymoreAffectedOtherBroker.brokerId} is not anymore affected by
                            |subscription to topic ${payload.topic}} from client ${message.clientIdentifier}""".trimMargin())
            // TODO send forward unsubscribe operation
        }

        /*****************************************************************
         * Response
         ****************************************************************/

        // send response
        val response = InternalServerMessage(message.clientIdentifier,
                ControlPacketType.SUBACK,
                SUBACKPayload(reasonCode))
        logger.trace("Sending response $response")
        response.zMsg.send(clients)
    }

    override fun processUNSUBSCRIBE(message: InternalServerMessage, clients: ZMQ.Socket, brokers: ZMQ.Socket) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun processPUBLISH(message: InternalServerMessage, clients: ZMQ.Socket, brokers: ZMQ.Socket) {
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

    override fun processBrokerForwardPublish(message: InternalServerMessage, clients: ZMQ.Socket, brokers: ZMQ.Socket) {
        logger.warn("Unsupported operation, message is discarded")
    }

    /**
     * Creates a subscription for a client based on information forwarded by another a broker.
     * Before creating the subscription, we need to make sure that the client is present in the client directory (as
     * a remote client).
     *
     * In theory, it is sufficient to only create the subscription in the RasterEntries that are inside the broker's
     * area.
     * However, to simplify this method the subscription is added to all RasterEntries intersecting with the given
     * geofence's outer bounding box, as this does not affect the result. I.e., as only publishers whose location is
     * inside the broker's area are communicating with this broker, all RasterEntries outside of the broker area are not
     * used anyways.
     */
    fun processBrokerForwardSubscribe(message: InternalServerMessage, clients: ZMQ.Socket, brokers: ZMQ.Socket) {
        val payload = message.payload.brokerForwardSubscribePayload.get()

        // the id is determined by ZeroMQ based on the first frame, so here it is the id of the forwarding broker
        val otherBrokerId = message.clientIdentifier
        logger.trace("Processing BrokerForwardSubscribe from broker {}", otherBrokerId)

        // make sure client exists as he did not send a connect
        if (!clientDirectory.clientExists(payload.clientIdentifier)) {
            // add client to directory as remote client
            clientDirectory.addClient(payload.clientIdentifier, Location.undefined())
        }

        // now we can do local subscribe
        val reasonCode = subscribeAtLocalBroker(message.clientIdentifier,
                clientDirectory,
                topicAndGeofenceMapper,
                payload.getSubscribePayload().topic,
                payload.getSubscribePayload().geofence,
                logger)

        val response = InternalServerMessage(otherBrokerId, ControlPacketType.SUBACK, SUBACKPayload(reasonCode))

        // acknowledge subscribe operation to other broker, he does not expect a particular message so we just reply
        // with the response that we have generated anyways (needs to go via the clients socket as response has to
        // go out of the ZMQProcess_Server
        logger.trace("Sending response with reason code $reasonCode")
        response.zMsg.send(clients)
    }

    /*****************************************************************
     * Message Processing Helper
     ****************************************************************/

    /**
     * Checks whether this particular broker is responsible for the client with the given [clientLocation]. If not, sends a
     * disconnect message and information about the responsible broker, if any exists. The client is also removed from
     * the client directory. Otherwise, does nothing.
     *
     * TODO removing a client from the client directly requires notifying all remote brokers of the same matter
     * (forwardDisconnect?)
     *
     * @return true, if this broker is responsible, otherwise false
     */
    private fun handleResponsibility(clientIdentifier: String, clientLocation: Location, clients: ZMQ.Socket): Boolean {
        if (!brokerAreaManager.checkIfOurAreaContainsLocation(clientLocation)) {
            // get responsible broker
            val repBroker = brokerAreaManager.getOtherBrokersContainingLocation(clientLocation)

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