package de.hasenburg.geobroker.server.matching

import de.hasenburg.geobroker.commons.model.message.*
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper
import de.hasenburg.geobroker.server.storage.client.ClientDirectory
import kotlinx.serialization.json.Json
import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.logging.log4j.Logger
import org.zeromq.ZMQ.Socket

/**
 * We supply a [Json] serialization object, because re-using it is faster than creating new ones.
 *
 * TODO: rather then handing in clients and brokers sockets, each method should return message that should be send via clients and via brokers socket.
 * TODO: change to abstract class and provide json so that it does not need to be an argument.
 */
interface IMatchingLogic {

    fun processCONNECT(clientIdentifier: String, payload: Payload.CONNECTPayload, clients: Socket, json: Json)

    fun processDISCONNECT(clientIdentifier: String, payload: Payload.DISCONNECTPayload, clients: Socket, json: Json)

    fun processPINGREQ(clientIdentifier: String, payload: Payload.PINGREQPayload, clients: Socket, json: Json)

    fun processSUBSCRIBE(clientIdentifier: String, payload: Payload.SUBSCRIBEPayload, clients: Socket, json: Json)

    fun processUNSUBSCRIBE(clientIdentifier: String, payload: Payload.UNSUBSCRIBEPayload, clients: Socket, json: Json)

    fun processPUBLISH(clientIdentifier: String, payload: Payload.PUBLISHPayload, clients: Socket, json: Json)

}

/*****************************************************************
 * Common Matching Tasks
 ****************************************************************/

fun connectClientAtLocalBroker(clientIdentifier: String,
                               location: Location?,
                               clientDirectory: ClientDirectory,
                               logger: Logger): Payload {

    val success = clientDirectory.addClient(clientIdentifier, location)

    return if (success) {
        logger.debug("Created client {}, acknowledging.", clientIdentifier)
        Payload.CONNACKPayload(ReasonCode.Success)
    } else {
        logger.debug("Client {} already exists, so protocol error. Disconnecting.", clientIdentifier)
        clientDirectory.removeClient(clientIdentifier)
        Payload.DISCONNECTPayload(ReasonCode.ProtocolError)
    }
}

fun updateClientLocationAtLocalBroker(clientIdentifier: String,
                                      location: Location?,
                                      clientDirectory: ClientDirectory,
                                      logger: Logger): ReasonCode {

    val success = clientDirectory.updateClientLocation(clientIdentifier, location)
    return if (success) {
        logger.debug("Updated location of {} to {}", clientIdentifier, location)
        ReasonCode.LocationUpdated
    } else {
        logger.debug("Client {} is not connected", clientIdentifier)
        ReasonCode.NotConnectedOrNoLocation
    }

}

fun subscribeAtLocalBroker(clientIdentifier: String,
                           clientDirectory: ClientDirectory,
                           topicAndGeofenceMapper: TopicAndGeofenceMapper,
                           topic: Topic,
                           geofence: Geofence,
                           logger: Logger): ReasonCode {

    val subscribed: ImmutablePair<ImmutablePair<String, Int>, Geofence>? =
            clientDirectory.checkIfSubscribed(clientIdentifier, topic, geofence)

    // if already subscribed -> remove subscription id from now unrelated geofence parts
    subscribed?.let { topicAndGeofenceMapper.removeSubscriptionId(subscribed.left, topic, subscribed.right) }

    val subscriptionId = clientDirectory.updateSubscription(clientIdentifier, topic, geofence)

    return if (subscriptionId == null) {
        logger.debug("Client {} is not connected", clientIdentifier)
        ReasonCode.NotConnectedOrNoLocation
    } else {
        topicAndGeofenceMapper.putSubscriptionId(subscriptionId, topic, geofence)
        logger.debug("Client {} subscribed to topic {} and geofence {}", clientIdentifier, topic, geofence)
        ReasonCode.GrantedQoS0
    }
}

fun unsubscribeAtLocalBroker(clientIdentifier: String,
                             clientDirectory: ClientDirectory,
                             topicAndGeofenceMapper: TopicAndGeofenceMapper,
                             topic: Topic,
                             logger: Logger): ReasonCode {
    var reasonCode = ReasonCode.Success

    // unsubscribe from client directory -> get subscription id
    val s = clientDirectory.removeSubscription(clientIdentifier, topic)

    // remove from storage if existed
    if (s != null) {
        topicAndGeofenceMapper.removeSubscriptionId(s.subscriptionId, s.topic, s.geofence)
        logger.debug("Client $clientIdentifier unsubscribed from $topic topic, subscription had the id ${s.subscriptionId}")
    } else {
        logger.debug("Client $clientIdentifier has no subscription with topic $topic, thus unable to unsubscribe")
        reasonCode = ReasonCode.NoSubscriptionExisted
    }

    return reasonCode
}

/**
 * @param publisherLocation - the location of the publisher
 */
fun publishMessageToLocalClients(publisherLocation: Location, publishPayload: Payload.PUBLISHPayload,
                                 clientDirectory: ClientDirectory, topicAndGeofenceMapper: TopicAndGeofenceMapper,
                                 clients: Socket, logger: Logger, json: Json): ReasonCode {

    logger.debug("Publishing topic {} to all subscribers", publishPayload.topic)

    // get subscriptions that have a geofence containing the publisher location
    val subscriptionIdResults =
            topicAndGeofenceMapper.getSubscriptionIds(publishPayload.topic, publisherLocation, clientDirectory)


    // only keep subscription if subscriber location is insider message geofence
    val subscriptionIds = subscriptionIdResults.filter { subId ->
        publishPayload.geofence.contains(clientDirectory.getClientLocation(subId.left))
    }

    // publish message to remaining subscribers
    for (subscriptionId in subscriptionIds) {
        val subscriberClientIdentifier = subscriptionId.left
        logger.debug("Client {} is a subscriber", subscriberClientIdentifier)
        val toPublish = publishPayload.toZMsg(json, subscriberClientIdentifier)
        logger.trace("Publishing $toPublish")
        toPublish.send(clients)
    }

    return if (subscriptionIds.isEmpty()) {
        logger.debug("No subscriber exists.")
        ReasonCode.NoMatchingSubscribers
    } else {
        ReasonCode.Success
    }
}
