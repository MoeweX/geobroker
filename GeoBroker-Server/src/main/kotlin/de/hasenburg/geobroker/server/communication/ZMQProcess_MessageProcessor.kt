@file:Suppress("ClassName")

package de.hasenburg.geobroker.server.communication

import de.hasenburg.geobroker.commons.communication.ZMQControlUtility
import de.hasenburg.geobroker.commons.communication.ZMQProcess
import de.hasenburg.geobroker.commons.model.message.Payload
import de.hasenburg.geobroker.commons.model.message.toPayloadAndId
import de.hasenburg.geobroker.server.matching.IMatchingLogic
import io.prometheus.client.Gauge
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration
import org.apache.logging.log4j.LogManager
import org.zeromq.SocketType
import org.zeromq.ZContext
import org.zeromq.ZMQ.Socket
import org.zeromq.ZMsg
import kotlin.system.exitProcess

private val logger = LogManager.getLogger()

/**
 * @param brokerId - identity should be the broker id this message processor is running on
 * @param number - incrementing number for this message processor (as there might be many), starts with 1
 */
class ZMQProcess_MessageProcessor(private val brokerId: String, private val number: Int,
                                  private val matchingLogic: IMatchingLogic) :
    ZMQProcess(getMessageProcessorIdentity(brokerId, number)) {

    // Prometheus Gauge
    private val util = Gauge.build().name("Geo_MessageProcessor_${brokerId}_${number}_util")
        .help("Utilization of the ZMQ message processor with the broker #$brokerId, number #$number").register()

    private val json = Json(JsonConfiguration.Stable) // so that it can be reused

    var numberOfProcessedMessages = 0
        private set

    // socket index
    private val processorIndex = 0

    override fun bindAndConnectSockets(context: ZContext): List<Socket> {
        val socketArray = arrayOfNulls<Socket>(1)

        val processor = context.createSocket(SocketType.DEALER)
        processor.identity = identity.toByteArray()
        processor.connect("inproc://" + ZMQProcess_Server.getServerIdentity(brokerId))
        socketArray[processorIndex] = processor

        // validate that we did not forget to set any sockets
        if (socketArray.any { it == null }) {
            logger.fatal("ZMQMessageProcessor does not add all sockets to socket list, shutting down")
            exitProcess(1)
        }

        return socketArray.map { it!! } // we might do !! because we checked above
    }

    override fun processZMQControlCommandOtherThanKill(zmqControlCommand: ZMQControlUtility.ZMQControlCommand,
                                                       msg: ZMsg) {
        // no other commands are of interest
    }

    override fun processZMsg(socketIndex: Int, msg: ZMsg) {

        if (socketIndex != processorIndex) {
            logger.error("Cannot process message for socket at index {}, as this index is not known.", socketIndex)
        }

        // start processing the message
        numberOfProcessedMessages++

        val message = msg.toPayloadAndId(json)

        logger.trace("ZMQProcess_MessageProcessor {} processing message number {}",
                identity,
                numberOfProcessedMessages)

        if (message != null) {
            val clientsSocket = sockets[processorIndex]

            // smart cast does not work here -> Payload defined in different module
            when (message.second) {
                is Payload.CONNECTPayload -> matchingLogic.processCONNECT(message.first,
                        message.second as Payload.CONNECTPayload,
                        clientsSocket,
                        json)
                is Payload.DISCONNECTPayload -> matchingLogic.processDISCONNECT(message.first,
                        message.second as Payload.DISCONNECTPayload,
                        clientsSocket,
                        json)
                is Payload.PINGREQPayload -> matchingLogic.processPINGREQ(message.first,
                        message.second as Payload.PINGREQPayload,
                        clientsSocket,
                        json)
                is Payload.SUBSCRIBEPayload -> matchingLogic.processSUBSCRIBE(message.first,
                        message.second as Payload.SUBSCRIBEPayload,
                        clientsSocket,
                        json)
                is Payload.UNSUBSCRIBEPayload -> matchingLogic.processUNSUBSCRIBE(message.first,
                        message.second as Payload.UNSUBSCRIBEPayload,
                        clientsSocket,
                        json)
                is Payload.PUBLISHPayload -> matchingLogic.processPUBLISH(message.first,
                        message.second as Payload.PUBLISHPayload,
                        clientsSocket,
                        json)
                is Payload.CONNACKPayload -> logger.warn("CONNACK messages are ignored by server")
                is Payload.PINGRESPPayload -> logger.warn("PINGRESP messages are ignored by server")
                is Payload.SUBACKPayload -> logger.warn("SUBACK messages are ignored by server")
                is Payload.UNSUBACKPayload -> logger.warn("UNSUBACK messages are ignored by server")
                is Payload.PUBACKPayload -> logger.warn("PUBACK messages are ignored by server")
            }
            logger.trace("Message successfully processed")

        } else {
            logger.warn("Received an incompatible message: {}", msg)
        }

    }

    override fun utilizationCalculated(utilization: Double) {
        util.set(utilization);
        logger.info("Current Utilization is {}%", utilization)
    }

    override fun shutdownCompleted() {
        logger.info("Shut down ZMQProcess_MessageProcessor {}", getMessageProcessorIdentity(identity, number))
    }

}

fun getMessageProcessorIdentity(brokerId: String, number: Int): String {
    return "$brokerId-message_processor-$number"
}
