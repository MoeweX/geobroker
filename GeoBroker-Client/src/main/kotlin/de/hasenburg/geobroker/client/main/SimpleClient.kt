package de.hasenburg.geobroker.client.main

import de.hasenburg.geobroker.client.communication.ZMQProcess_SimpleClient
import de.hasenburg.geobroker.commons.*
import de.hasenburg.geobroker.commons.communication.ZMQControlUtility
import de.hasenburg.geobroker.commons.communication.ZMQProcessManager
import de.hasenburg.geobroker.commons.model.KryoSerializer
import de.hasenburg.geobroker.commons.model.message.*
import de.hasenburg.geobroker.commons.model.message.Payload.*
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import org.apache.logging.log4j.LogManager
import org.zeromq.SocketType
import org.zeromq.ZMQ
import org.zeromq.ZMsg
import kotlin.system.exitProcess

private val logger = LogManager.getLogger()

fun main() {
    val processManager = ZMQProcessManager()
    val client = SimpleClient("localhost", 5559, processManager)

    // connect
    client.send(CONNECTPayload(Location.random()))

    // receive one message
    logger.info("Received server answer: {}", client.receive())

    // subscribe
    client.send(SUBSCRIBEPayload(Topic("test"), Geofence.circle(Location.random(), 2.0)))

    // receive one message
    logger.info("Received server answer: {}", client.receive())

    // wait 5 seconds
    sleepNoLog(5000, 0)

    // disconnect
    client.send(DISCONNECTPayload(ReasonCode.NormalDisconnection))

    client.tearDownClient()
    if (processManager.tearDown(3000)) {
        logger.info("SimpleClient shut down properly.")
    } else {
        logger.fatal("ProcessManager reported that processes are still running: {}",
                processManager.incompleteZMQProcesses)
    }
    exitProcess(0)
}

class SimpleClient(address: String, port: Int, private val processManager: ZMQProcessManager,
                   val identity: String = "SimpleClient-" + System.nanoTime()) {

    private val kryo = KryoSerializer()
    private val orderSocket: ZMQ.Socket

    init {
        processManager.submitZMQProcess(identity, ZMQProcess_SimpleClient(address, port, identity))
        orderSocket = processManager.context.createSocket(SocketType.REQ)
        orderSocket.identity = identity.toByteArray()
        orderSocket.connect(generateClientOrderBackendString(identity))

        logger.info("Created client {}", identity)
    }

    fun tearDownClient() {
        orderSocket.linger = 0
        processManager.context.destroySocket(orderSocket)
        processManager.sendCommandToZMQProcess(identity, ZMQControlUtility.ZMQControlCommand.KILL)
    }

    fun send(payload: Payload): ZMsg {
        val orderMessage = ZMsg.newStringMsg(ZMQProcess_SimpleClient.ORDERS.SEND.name)
        val payloadMessage = payloadToZMsg(payload, kryo)
        repeat(payloadMessage.size) {
            orderMessage.add(payloadMessage.pop())
        }

        orderMessage.send(orderSocket)
        return ZMsg.recvMsg(orderSocket)
    }

    fun receive(): Payload {
        val orderMessage = ZMsg.newStringMsg(ZMQProcess_SimpleClient.ORDERS.RECEIVE.name)

        // send order
        orderMessage.send(orderSocket)

        return ZMsg.recvMsg(orderSocket).transformZMsg(kryo)!! // was validated before order socket
    }

    /**
     * @param timeout - receive timeout in ms
     * @return a message from the server or null if server did not send a message
     */
    fun receiveWithTimeout(timeout: Int): Payload? {
        val orderMessage =
                ZMsg.newStringMsg(ZMQProcess_SimpleClient.ORDERS.RECEIVE_WITH_TIMEOUT.name, timeout.toString() + "")

        // send order
        orderMessage.send(orderSocket)
        val response = ZMsg.recvMsg(orderSocket)
        // check first frame as might be empty
        return if (ZMQProcess_SimpleClient.ORDERS.EMPTY.name == response.first!!.getString(ZMQ.CHARSET)) {
            null
        } else response.transformZMsg(kryo)

    }

}
