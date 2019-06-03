package de.hasenburg.geobroker.server.matching

import de.hasenburg.geobroker.commons.model.message.ControlPacketType
import de.hasenburg.geobroker.commons.model.message.ReasonCode
import de.hasenburg.geobroker.commons.model.message.payloads.DISCONNECTPayload
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.server.communication.InternalServerMessage
import de.hasenburg.geobroker.server.distribution.BrokerAreaManager
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper
import de.hasenburg.geobroker.server.storage.client.ClientDirectory
import org.apache.logging.log4j.LogManager
import org.zeromq.ZMQ

private val logger = LogManager.getLogger()

class DisGBAtPublisherMatchingLogic constructor(private val clientDirectory: ClientDirectory,
                                                private val topicAndGeofenceMapper: TopicAndGeofenceMapper,
                                                private val brokerAreaManager: BrokerAreaManager) : IMatchingLogic {


    override fun processCONNECT(message: InternalServerMessage, clients: ZMQ.Socket, brokers: ZMQ.Socket) {
        val payload = message.payload.connectPayload.get()

        if (!handleResponsibility(message.clientIdentifier, payload.location, clients)) {
            return  // we are not responsible, client has been notified
        }

        val response = connectClientAtLocalBroker(message.clientIdentifier,
                payload.location,
                clientDirectory,
                logger)

        logger.trace("Sending response $response")
        response.zMsg.send(clients)
    }

    override fun processDISCONNECT(message: InternalServerMessage, clients: ZMQ.Socket, brokers: ZMQ.Socket) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun processPINGREQ(message: InternalServerMessage, clients: ZMQ.Socket, brokers: ZMQ.Socket) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun processSUBSCRIBE(message: InternalServerMessage, clients: ZMQ.Socket, brokers: ZMQ.Socket) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun processUNSUBSCRIBE(message: InternalServerMessage, clients: ZMQ.Socket, brokers: ZMQ.Socket) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun processPUBLISH(message: InternalServerMessage, clients: ZMQ.Socket, brokers: ZMQ.Socket) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun processBrokerForwardPublish(message: InternalServerMessage, clients: ZMQ.Socket, brokers: ZMQ.Socket) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
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
    private fun handleResponsibility(clientIdentifier: String, clientLocation: Location, clients: ZMQ.Socket): Boolean {
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