package de.hasenburg.geobroker.server.matching

import de.hasenburg.geobroker.server.communication.InternalServerMessage
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
