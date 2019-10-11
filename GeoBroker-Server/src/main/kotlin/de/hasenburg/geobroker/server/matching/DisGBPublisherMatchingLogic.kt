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
import de.hasenburg.geobroker.server.storage.client.SubscriptionAffection
import org.apache.logging.log4j.LogManager
import org.zeromq.ZMQ.Socket
import org.zeromq.ZMsg

private val logger = LogManager.getLogger()

class DisGBAtPublisherMatchingLogic constructor(private val clientDirectory: ClientDirectory,
                                                private val topicAndGeofenceMapper: TopicAndGeofenceMapper,
                                                private val brokerAreaManager: BrokerAreaManager) : IMatchingLogic {

    private val subscriptionAffection = SubscriptionAffection()

    private fun sendResponse(response: ZMsg, clients: Socket) {
        logger.trace("Sending response $response")
        response.send(clients)
    }

    override fun processCONNECT(clientIdentifier: String, payload: CONNECTPayload, clients: Socket,
                                brokers: Socket, kryo: KryoSerializer) {
        if (!payload.location.isUndefined && !handleResponsibility(clientIdentifier,
                        payload.location,
                        clients,
                        brokers,
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

        doDisconnect(clientIdentifier,
                DISCONNECTPayload(ReasonCode.NormalDisconnection),
                clients,
                brokers,
                kryo)

        logger.debug("Disconnected client {}, code {}", clientIdentifier, payload.reasonCode)

    }

    override fun processPINGREQ(clientIdentifier: String, payload: PINGREQPayload, clients: Socket,
                                brokers: Socket, kryo: KryoSerializer) {

        var reasonCode = ReasonCode.LocationUpdated

        /* ***************************************************************
         * Local location update
         ****************************************************************/

        val success = clientDirectory.updateClientLocation(clientIdentifier, payload.location)
        if (success) {
            logger.debug("Updated location of {} to {}", clientIdentifier, payload.location)
        } else {
            logger.debug("Client {} is not connected", clientIdentifier)
            reasonCode = ReasonCode.NotConnected
        }

        /* ***************************************************************
         * Forwarding to other brokers
         ****************************************************************/

        // only if we were able to update the location locally, the request should be forwarded
        if (success) {
            // determine other brokers that are affected by any of the clients subscriptions
            val clientAffections = subscriptionAffection.getAffections(clientIdentifier)

            // forward location to all affected brokers
            for (otherAffectedBroker in clientAffections) {
                logger.debug("""|Broker area of ${otherAffectedBroker.brokerId} is affected by the location update
                                |of client $clientIdentifier""".trimMargin())
                // send message to BrokerCommunicator who takes care of the rest
                ZMQProcess_BrokerCommunicator.generatePULLSocketMessage(otherAffectedBroker.brokerId,
                        payloadToZMsg(payload, kryo)).send(brokers)
            }
        }

        /* ***************************************************************
         * Response
         ****************************************************************/

        val response = payloadToZMsg(PINGRESPPayload(reasonCode), kryo, clientIdentifier)
        sendResponse(response, clients)
    }

    override fun processSUBSCRIBE(clientIdentifier: String, payload: SUBSCRIBEPayload, clients: Socket,
                                  brokers: Socket, kryo: KryoSerializer) {

        /* ***************************************************************
         * Local Things
         * - done first to create subscription if did not exist
         * - we always create a subscription, even if not in area, so that the "main broker" knows about any
         * - to simplify code, the subscription id is also added to raster entries outside of the broker area as this
         * does not affect the result (see [processBrokerForwardSubscribe] for a longer explanation)
         ****************************************************************/

        val reasonCode = subscribeAtLocalBroker(clientIdentifier,
                clientDirectory,
                topicAndGeofenceMapper,
                payload.topic,
                payload.geofence,
                logger)
        val subscriptionId = clientDirectory.getSubscription(clientIdentifier, payload.topic)?.subscriptionId
        val clientLocation = clientDirectory.getClientLocation(clientIdentifier) // might be needed for remote

        /* ***************************************************************
         * Remote Things
         ****************************************************************/

        // only if a subscription was created/updated locally, it should be forwarded
        if (subscriptionId != null && clientLocation != null) {
            // calculate what brokers are affected by the subscription's geofence
            val otherAffectedBrokers = brokerAreaManager.getOtherBrokersIntersectingWithGeofence(payload.geofence)

            // forward subscribe to all currently affected brokers
            for (otherAffectedBroker in otherAffectedBrokers) {
                logger.debug("""|Broker area of ${otherAffectedBroker.brokerId} intersects with subscription to topic
                            |${payload.topic}} from client $clientIdentifier""".trimMargin())
                // send message to BrokerCommunicator who takes care of the rest
                ZMQProcess_BrokerCommunicator.generatePULLSocketMessage(otherAffectedBroker.brokerId,
                        payloadToZMsg(BrokerForwardSubscribePayload(clientIdentifier, payload), kryo)).send(brokers)
            }

            // all brokers that did not know the client before have to also receive the client location
            val newlyAffectedBrokers = subscriptionAffection.determineAffectedBrokersThatDoNotKnowTheClient(
                    subscriptionId,
                    otherAffectedBrokers)
            for (newlyAffectedBroker in newlyAffectedBrokers) {
                logger.debug("""|Broker ${newlyAffectedBroker.brokerId} did not know client $clientIdentifier
                                |before, so also sending its most up to date location""".trimMargin())
                // send message to BrokerCommunicator who takes care of the rest
                ZMQProcess_BrokerCommunicator.generatePULLSocketMessage(newlyAffectedBroker.brokerId,
                        payloadToZMsg(BrokerForwardPingreqPayload(clientIdentifier, PINGREQPayload(clientLocation)),
                                kryo)).send(brokers)
            }

            // update broker affection -> returns now not anymore affected brokers
            val notAnymoreAffectedOtherBrokers =
                    subscriptionAffection.updateAffections(subscriptionId, otherAffectedBrokers)

            // unsubscribe these now not anymore affected brokers
            for (notAnymoreAffectedOtherBroker in notAnymoreAffectedOtherBrokers) {
                logger.debug("""|Broker area of ${notAnymoreAffectedOtherBroker.brokerId} is not anymore affected by
                                |subscription to topic ${payload.topic}} from client $clientIdentifier""".trimMargin())
                val unsubPayload = UNSUBSCRIBEPayload(payload.topic)
                // send message to BrokerCommunicator who takes care of the rest
                ZMQProcess_BrokerCommunicator.generatePULLSocketMessage(notAnymoreAffectedOtherBroker.brokerId,
                        payloadToZMsg(BrokerForwardUnsubscribePayload(clientIdentifier, unsubPayload), kryo))
                    .send(brokers)
            }
        }

        /* ***************************************************************
         * Response
         ****************************************************************/

        val response = payloadToZMsg(SUBACKPayload(reasonCode), kryo, clientIdentifier)
        sendResponse(response, clients)
    }

    override fun processUNSUBSCRIBE(clientIdentifier: String, payload: UNSUBSCRIBEPayload, clients: Socket,
                                    brokers: Socket, kryo: KryoSerializer) {

        /* ***************************************************************
         * Local unsubscribe
         ****************************************************************/

        val subscription = clientDirectory.getSubscription(clientIdentifier, payload.topic)
        val reasonCode = unsubscribeAtLocalBroker(clientIdentifier,
                clientDirectory,
                topicAndGeofenceMapper,
                payload.topic,
                logger)


        /* ***************************************************************
         * Forwarding to other brokers
         ****************************************************************/

        // only if a subscription existed locally, the request should be forwarded
        if (subscription != null) {
            // determine other brokers that were affected by the subscription
            val clientAffections = subscriptionAffection.getAffections(subscription.subscriptionId)

            // forward unsubscribe
            for (otherAffectedBroker in clientAffections) {
                logger.debug("""|Broker area of ${otherAffectedBroker.brokerId} is affected by the unsubscribe from
                                |topic $payload.topic of client $clientIdentifier""".trimMargin())
                // send message to BrokerCommunicator who takes care of the rest
                ZMQProcess_BrokerCommunicator.generatePULLSocketMessage(otherAffectedBroker.brokerId,
                        payloadToZMsg(BrokerForwardUnsubscribePayload(clientIdentifier, payload), kryo)).send(brokers)
            }
        }

        /* ***************************************************************
         * Response
         ****************************************************************/

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
            // get subscriptions that have a geofence containing the publisher location
            val subscriptionIdResults =
                    topicAndGeofenceMapper.getSubscriptionIds(payload.topic, publisherLocation, clientDirectory)

            // only keep subscription if subscriber location is insider message geofence
            val subscriptionIds = subscriptionIdResults.filter { subId ->
                payload.geofence.contains(clientDirectory.getClientLocation(subId.left)!!)
            }

            val remoteClientIds = mutableMapOf<String, MutableList<String>>()

            // publish message to remaining subscribers
            for (subscriptionId in subscriptionIds) {
                val subscriber = clientDirectory.getClient(subscriptionId.left)

                when {
                    subscriber == null -> // in very rare cases another thread removed it again already, so do nothing
                        logger.warn("A Subscriber disconnected before being able to publish an outstanding message")
                    subscriber.remote -> {
                        logger.debug("Client {} is a remote subscriber", subscriber.clientIdentifier)
                        // remote client -> must be send to his broker
                        val otherBrokerId: String? =
                                brokerAreaManager.getOtherBrokerContainingLocation(subscriber.location)?.brokerId
                        if (otherBrokerId != null) {
                            logger.debug("""|Client ${subscriber.clientIdentifier} is connected to broker $otherBrokerId,
                                            |thus forwarding the published message (topic = ${payload.topic}) to it""".trimMargin())
                            remoteClientIds.getOrPut(otherBrokerId) { mutableListOf() }.add(subscriber.clientIdentifier)
                        }
                    }
                    else -> {
                        // local client -> send directly
                        logger.debug("Client {} is a local subscriber", subscriber.clientIdentifier)
                        val toPublish = payloadToZMsg(payload, kryo, subscriber.clientIdentifier)
                        logger.trace("Publishing $toPublish")
                        toPublish.send(clients)
                    }
                }

            }

            // forward message to remote brokers together with all their subscribers
            for ((otherBrokerId, subscribers) in remoteClientIds) {
                // send message to BrokerCommunicator who takes care of the rest
                ZMQProcess_BrokerCommunicator.generatePULLSocketMessage(otherBrokerId,
                        payloadToZMsg(BrokerForwardPublishPayload(payload, subscriberClientIdentifiers = subscribers),
                                kryo)).send(brokers)
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

    /**
     * Disconnects a (remote) client from this broker, i.e., the client is connected to another broker and got
     * disconnected there.
     *
     * Actually checks whether the given client is a remote client before disconnecting.
     */
    override fun processBrokerForwardDisconnect(otherBrokerId: String,
                                                payload: BrokerForwardDisconnectPayload, clients: Socket,
                                                brokers: Socket, kryo: KryoSerializer) {

        var reasonCode = ReasonCode.WrongBroker // i.e., not a remote client

        // the id is determined by ZeroMQ based on the first frame, so here it is the id of the forwarding broker
        logger.trace("Processing BrokerForwardSubscribe from broker {}", otherBrokerId)

        if (clientDirectory.clientExistsAsRemoteClient(payload.clientIdentifier)) {
            reasonCode = ReasonCode.Success
            clientDirectory.removeClient(payload.clientIdentifier)
        }

        val response = payloadToZMsg(DISCONNECTPayload(reasonCode), kryo, otherBrokerId)

        // acknowledge disconnect operation to other broker, he does not expect a particular message (needs to go via
        // the clients socket as response has to go out of the ZMQProcess_Server
        logger.trace("Sending disconnect response to broker $otherBrokerId with reason code $reasonCode")
        response.send(clients)
    }

    /**
     * Updates the location and heartbeat of a client based on information forwarded by another broker.
     * Before doing so, we need to make sure the client is present in the client directory (as a remote client).
     */
    override fun processBrokerForwardPingreq(otherBrokerId: String, payload: BrokerForwardPingreqPayload,
                                             clients: Socket, brokers: Socket, kryo: KryoSerializer) {

        // the id is determined by ZeroMQ based on the first frame, so here it is the id of the forwarding broker
        logger.trace("Processing BrokerForwardSubscribe from broker {}", otherBrokerId)

        // make sure client exists as he did not send a connect
        if (!clientDirectory.clientExists(payload.clientIdentifier)) {
            // add client to directory as remote client
            clientDirectory.addClient(payload.clientIdentifier, Location.undefined(), true)
        }

        // now we can do local location update
        val reasonCode = updateClientLocationAtLocalBroker(payload.clientIdentifier,
                payload.pingreqPayload.location,
                clientDirectory,
                logger)

        val response = payloadToZMsg(PINGRESPPayload(reasonCode), kryo, otherBrokerId)

        // acknowledge subscribe operation to other broker, he does not expect a particular message so we just reply
        // with the response that we have generated anyways (needs to go via the clients socket as response has to
        // go out of the ZMQProcess_Server
        sendResponse(response, clients)

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
    override fun processBrokerForwardSubscribe(otherBrokerId: String, payload: BrokerForwardSubscribePayload,
                                               clients: Socket, brokers: Socket, kryo: KryoSerializer) {

        // the id is determined by ZeroMQ based on the first frame, so here it is the id of the forwarding broker
        logger.trace("Processing BrokerForwardSubscribe from broker {}", otherBrokerId)

        // make sure client exists as he did not send a connect
        if (!clientDirectory.clientExists(payload.clientIdentifier)) {
            // add client to directory as remote client
            clientDirectory.addClient(payload.clientIdentifier, Location.undefined(), true)
        }

        // now we can do local subscribe
        val reasonCode = subscribeAtLocalBroker(payload.clientIdentifier,
                clientDirectory,
                topicAndGeofenceMapper,
                payload.subscribePayload.topic,
                payload.subscribePayload.geofence,
                logger)

        val response = payloadToZMsg(SUBACKPayload(reasonCode), kryo, otherBrokerId)

        // acknowledge subscribe operation to other broker, he does not expect a particular message so we just reply
        // with the response that we have generated anyways (needs to go via the clients socket as response has to
        // go out of the ZMQProcess_Server
        sendResponse(response, clients)
    }

    /**
     * Removes a subscription for a client based on information forwarded by another a broker.
     */
    override fun processBrokerForwardUnsubscribe(otherBrokerId: String,
                                                 payload: BrokerForwardUnsubscribePayload, clients: Socket,
                                                 brokers: Socket, kryo: KryoSerializer) {

        // the id is determined by ZeroMQ based on the first frame, so here it is the id of the forwarding broker
        logger.trace("Processing BrokerForwardSubscribe from broker {}", otherBrokerId)

        val reasonCode = unsubscribeAtLocalBroker(payload.clientIdentifier,
                clientDirectory,
                topicAndGeofenceMapper,
                payload.unsubscribePayload.topic,
                logger)

        val response = payloadToZMsg(UNSUBACKPayload(reasonCode), kryo, otherBrokerId)

        // acknowledge unsubscribe operation to other broker, he does not expect a particular message so we just reply
        // with the response that we have generated anyways (needs to go via the clients socket as response has to
        // go out of the ZMQProcess_Server
        sendResponse(response, clients)
    }

    /**
     * Publishes a message to local clients that originates from a client connected to another broker.
     *
     * As the other broker already did the matching, we can just deliver it instead of doing the matching again (in
     * case the clients exists)
     */
    override fun processBrokerForwardPublish(otherBrokerId: String, payload: BrokerForwardPublishPayload,
                                             clients: Socket, brokers: Socket, kryo: KryoSerializer) {

        val reasonCode = ReasonCode.Success

        // the id is determined by ZeroMQ based on the first frame, so here it is the id of the forwarding broker
        logger.trace("Processing BrokerForwardPublish from broker {}", otherBrokerId)

        // validate that target client is connected
        for (subscriberClientIdentifier in payload.subscriberClientIdentifiers) {
            if (clientDirectory.clientExists(subscriberClientIdentifier)) {
                logger.debug("Sending a message that was matched by broker $otherBrokerId to Client {}",
                        subscriberClientIdentifier)
                val toPublish = payloadToZMsg(payload.publishPayload, kryo, subscriberClientIdentifier)
                logger.trace("Publishing $toPublish")
                toPublish.send(clients)
            } else {
                logger.warn("Another broker matched a message for client {}, but he is not connected",
                        subscriberClientIdentifier)
                // TODO we could now change the reason code to something like partial success
            }
        }

        val response = payloadToZMsg(PUBACKPayload(reasonCode), kryo, otherBrokerId)

        // acknowledge publish operation to other broker, he does not expect a particular message so we just reply
        // with the response that we have generated anyways (needs to go via the clients socket as response has to
        // go out of the ZMQProcess_Server
        sendResponse(response, clients)
    }

    /*****************************************************************
     * Message Processing Helper
     ****************************************************************/

    /**
     * Checks whether this particular broker is responsible for the client with the given [clientLocation]. If not, sends a
     * disconnect message and information about the responsible broker, if any exists. The client is also removed from
     * the client directory. Otherwise, does nothing.
     *
     *
     * @return true, if this broker is responsible, otherwise false
     */
    private fun handleResponsibility(clientIdentifier: String, clientLocation: Location, clients: Socket,
                                     brokers: Socket, kryo: KryoSerializer): Boolean {
        if (!brokerAreaManager.checkIfOurAreaContainsLocation(clientLocation)) {
            // get responsible broker
            val repBroker = brokerAreaManager.getOtherBrokerContainingLocation(clientLocation)

            val response = payloadToZMsg(DISCONNECTPayload(ReasonCode.WrongBroker, repBroker), kryo, clientIdentifier)

            logger.debug("Not responsible for client {}, responsible broker is {}", clientIdentifier, repBroker)
            response.send(clients)

            // TODO F: migrate client data to other broker, right now he has to update the information himself
            logger.debug("Client had {} active subscriptions",
                    clientDirectory.getCurrentClientSubscriptions(clientIdentifier))

            // do disconnect and handle all with that related matters
            doDisconnect(clientIdentifier, DISCONNECTPayload(ReasonCode.WrongBroker), clients, brokers, kryo)

            return false
        }
        return true
    }

    /**
     * Does all things necessary for a client disconnect:
     * - remove the client from [clientDirectory]
     * - remove all subscriptions of the client from [subscriptionAffection]
     * - sent forward disconnect to all formerly affected brokers of any of the client's subscriptions
     * - TODO tell the client about disconnect (must also be added to all other MatchingLogics, add to [IMatchingLogic])
     *
     * @param clientIdentifier - identifier of disconnecting client
     * @param payload - disconnect payload with corresponding [ReasonCode]
     * @param clients - socket used for client communication
     * @param brokers - socket used for broker communication
     */
    private fun doDisconnect(clientIdentifier: String, payload: DISCONNECTPayload, clients: Socket, brokers: Socket,
                             kryo: KryoSerializer) {

        val success = clientDirectory.removeClient(clientIdentifier)
        if (!success) {
            logger.trace("Client for {} did not exist", clientIdentifier)
            return
        }

        val formerlyAffectedBrokers = subscriptionAffection.getAffections(clientIdentifier)
        subscriptionAffection.removeAffections(clientIdentifier)

        for (formerlyAffectedBroker in formerlyAffectedBrokers) {
            logger.trace("""|Broker area of ${formerlyAffectedBroker.brokerId} is notified about disconnect from client
                            |$clientIdentifier""".trimMargin())
            // send message to BrokerCommunicator who takes care of the rest
            ZMQProcess_BrokerCommunicator.generatePULLSocketMessage(formerlyAffectedBroker.brokerId,
                    payloadToZMsg(BrokerForwardDisconnectPayload(clientIdentifier, payload), kryo)).send(brokers)
        }

    }

}