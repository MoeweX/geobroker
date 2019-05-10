package de.hasenburg.geobroker.server.matching

import de.hasenburg.geobroker.commons.model.message.ControlPacketType
import de.hasenburg.geobroker.commons.model.message.ReasonCode
import de.hasenburg.geobroker.commons.model.message.Topic
import de.hasenburg.geobroker.commons.model.message.payloads.*
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.server.communication.InternalServerMessage
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper
import de.hasenburg.geobroker.server.storage.client.ClientDirectory
import org.apache.logging.log4j.Logger
import org.zeromq.ZMQ.Socket

/**
 * Message Processing Notes <br></br>
 * - we already validated the messages above using #buildMessage() <br></br>
 * -> we expect the payload to be compatible with the control packet type <br></br>
 * -> we expect all fields to be set
 */
interface IMatchingLogic {

    fun processCONNECT(message: InternalServerMessage, clients: Socket, brokers: Socket)

    fun processDISCONNECT(message: InternalServerMessage, clients: Socket, brokers: Socket)

    fun processPINGREQ(message: InternalServerMessage, clients: Socket, brokers: Socket)

    fun processSUBSCRIBE(message: InternalServerMessage, clients: Socket, brokers: Socket)

    fun processUNSUBSCRIBE(message: InternalServerMessage, clients: Socket, brokers: Socket)

    fun processPUBLISH(message: InternalServerMessage, clients: Socket, brokers: Socket)

    fun processBrokerForwardPublish(message: InternalServerMessage, clients: Socket, brokers: Socket)

}

/*****************************************************************
 * Common Matching Tasks
 ****************************************************************/

fun connectClientAtLocalBroker(clientIdentifier: String, location: Location,
                               clientDirectory: ClientDirectory, logger: Logger): InternalServerMessage {

    val success = clientDirectory.addClient(clientIdentifier, location)

    if (success) {
        logger.debug("Created client {}, acknowledging.", clientIdentifier)
        return InternalServerMessage(clientIdentifier,
                ControlPacketType.CONNACK,
                CONNACKPayload(ReasonCode.Success))
    } else {
        logger.debug("Client {} already exists, so protocol error. Disconnecting.", clientIdentifier)
        clientDirectory.removeClient(clientIdentifier)
        return InternalServerMessage(clientIdentifier,
                ControlPacketType.DISCONNECT,
                DISCONNECTPayload(ReasonCode.ProtocolError))
    }
}

fun updateClientLocationAtLocalBroker(clientIdentifier: String, location: Location,
                                      clientDirectory: ClientDirectory, logger: Logger): InternalServerMessage {

    val success = clientDirectory.updateClientLocation(clientIdentifier, location)
    if (success) {
        logger.debug("Updated location of {} to {}", clientIdentifier, location)

        return InternalServerMessage(clientIdentifier,
                ControlPacketType.PINGRESP,
                PINGRESPPayload(ReasonCode.LocationUpdated))
    } else {
        logger.debug("Client {} is not connected", clientIdentifier)
        return InternalServerMessage(clientIdentifier,
                ControlPacketType.PINGRESP,
                PINGRESPPayload(ReasonCode.NotConnected))
    }

}

fun subscribeAtLocalBroker(clientIdentifier: String, clientDirectory: ClientDirectory,
                           topicAndGeofenceMapper: TopicAndGeofenceMapper, topic: Topic,
                           geofence: Geofence, logger: Logger): InternalServerMessage {
    val subscribed = clientDirectory.checkIfSubscribed(
            clientIdentifier,
            topic,
            geofence)

    // if already subscribed -> remove subscription id from now unrelated geofence parts
    if (subscribed != null) {
        topicAndGeofenceMapper.removeSubscriptionId(subscribed.left, topic, subscribed.right)
    }

    val subscriptionId = clientDirectory.putSubscription(clientIdentifier,
            topic,
            geofence)

    if (subscriptionId == null) {
        logger.debug("Client {} is not connected", clientIdentifier)
        return InternalServerMessage(clientIdentifier,
                ControlPacketType.SUBACK,
                SUBACKPayload(ReasonCode.NotConnected))
    } else {
        topicAndGeofenceMapper.putSubscriptionId(subscriptionId, topic, geofence)
        logger.debug("Client {} subscribed to topic {} and geofence {}", clientIdentifier, topic, geofence)
        return InternalServerMessage(clientIdentifier,
                ControlPacketType.SUBACK,
                SUBACKPayload(ReasonCode.GrantedQoS0))
    }
}

/**
 * @param publisherLocation - the location of the publisher
 */
fun publishMessageToLocalClients(publisherLocation: Location, publishPayload: PUBLISHPayload,
                                 clientDirectory: ClientDirectory,
                                 topicAndGeofenceMapper: TopicAndGeofenceMapper, clients: Socket,
                                 logger: Logger): ReasonCode {

    logger.debug("Publishing topic {} to all subscribers", publishPayload.topic)

    // get subscriptions that have a geofence containing the publisher location
    val subscriptionIds = topicAndGeofenceMapper.getSubscriptionIds(publishPayload.topic, publisherLocation)

    // only keep subscription if subscriber location is insider message geofence
    subscriptionIds.removeIf { subId ->
        !publishPayload.geofence
                .contains(clientDirectory.getClientLocation(subId.left)!!)
    }

    // publish message to remaining subscribers
    for (subscriptionId in subscriptionIds) {
        val subscriberClientIdentifier = subscriptionId.left
        logger.debug("Client {} is a subscriber", subscriberClientIdentifier)
        val toPublish = InternalServerMessage(subscriberClientIdentifier,
                ControlPacketType.PUBLISH,
                publishPayload)
        logger.trace("Publishing $toPublish")
        toPublish.zMsg.send(clients)
    }

    if (subscriptionIds.isEmpty()) {
        logger.debug("No subscriber exists.")
        return ReasonCode.NoMatchingSubscribers
    } else {
        return ReasonCode.Success
    }

}