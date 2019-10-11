package de.hasenburg.geobroker.client.communication

import de.hasenburg.geobroker.commons.*
import de.hasenburg.geobroker.commons.communication.ZMQControlUtility
import de.hasenburg.geobroker.commons.communication.ZMQProcess
import de.hasenburg.geobroker.commons.model.KryoSerializer
import de.hasenburg.geobroker.commons.model.message.Payload
import de.hasenburg.geobroker.commons.model.message.payloadToZMsg
import de.hasenburg.geobroker.commons.model.message.transformZMsg
import org.apache.logging.log4j.LogManager
import org.zeromq.SocketType
import org.zeromq.ZContext
import org.zeromq.ZMQ.Socket
import org.zeromq.ZMsg

import java.util.ArrayList
import kotlin.system.exitProcess

private val logger = LogManager.getLogger()

@Suppress("ClassName")
class ZMQProcess_SimpleClient(private val address: String, private val port: Int, identity: String) :
    ZMQProcess(identity) {

    enum class ORDERS {
        SEND, RECEIVE, RECEIVE_WITH_TIMEOUT, CONFIRM, FAIL, EMPTY
    }

    // Client processing backend, accepts REQ and answers with REP
    private val CLIENT_ORDER_BACKEND = generateClientOrderBackendString(identity)
    var kryo = KryoSerializer()

    // socket indices
    private val ORDER_INDEX = 0
    private val SERVER_INDEX = 1

    // received message buffer
    private val receivedMessages: MutableList<Payload> = ArrayList()

    override fun bindAndConnectSockets(context: ZContext): List<Socket> {
        val socketArray = arrayOfNulls<Socket>(2)

        val orders = context.createSocket(SocketType.REP)
        orders.bind(CLIENT_ORDER_BACKEND)
        socketArray[ORDER_INDEX] = orders

        val serverSocket = context.createSocket(SocketType.DEALER)
        serverSocket.identity = identity.toByteArray()
        serverSocket.connect("tcp://$address:$port")
        socketArray[SERVER_INDEX] = serverSocket

        // validate that we did not forget to set any sockets
        if (socketArray.any { it == null }) {
            logger.fatal("ZMQSimpleClient does not add all sockets to socket list, shutting down")
            exitProcess(1)
        }

        return socketArray.map { it!! } // we might do !! because we checked above
    }

    override fun processZMQControlCommandOtherThanKill(zmqControlCommand: ZMQControlUtility.ZMQControlCommand,
                                                       msg: ZMsg) {
        // no other commands are of interest
    }

    override fun processZMsg(socketIndex: Int, msg: ZMsg) {

        when (socketIndex) {
            SERVER_INDEX -> { // got a reply from the server
                logger.trace("Received message from server.")
                val serverMessage = msg.transformZMsg(kryo)
                if (serverMessage != null) {
                    receivedMessages.add(serverMessage)
                } else {
                    logger.warn("Server message malformed or empty")
                }
            }
            ORDER_INDEX -> { // got the order to do something
                var valid = true
                if (msg.size < 1) {
                    logger.warn("Order has the wrong length {}", msg)
                    valid = false
                }
                val orderType = msg.popString()

                if (valid && ORDERS.RECEIVE.name == orderType) {
                    logger.trace("ORDER = Receive from Broker")

                    if (receivedMessages.size > 0) {
                        val payload = receivedMessages.removeAt(0)
                        payloadToZMsg(payload, kryo).send(sockets[ORDER_INDEX])
                    } else {
                        // nothing received yet, so let's wait
                        val payload = ZMsg.recvMsg(sockets[SERVER_INDEX], true).transformZMsg(kryo)
                        if (payload != null) {
                            payloadToZMsg(payload, kryo).send(sockets[ORDER_INDEX])
                        } else {
                            logger.warn("Server message malformed or empty")
                            valid = false
                        }
                    }
                } else if (valid && ORDERS.RECEIVE_WITH_TIMEOUT.name == orderType) {
                    var timeout = 0
                    try {
                        timeout = Integer.parseInt(msg.popString()!!)
                    } catch (e: NumberFormatException) {
                        logger.warn("Receive with timeout did not contain a proper timeout, setting to 0ms")
                    } catch (e: NullPointerException) {
                        logger.warn("Receive with timeout did not contain a proper timeout, setting to 0ms")
                    }

                    logger.trace("ORDER = Receive from Broker (with timeout {})", timeout)

                    // first check the buffer
                    if (receivedMessages.size > 0) {
                        val payload = receivedMessages.removeAt(0)
                        payloadToZMsg(payload, kryo).send(sockets[ORDER_INDEX])
                        return
                    } else {
                        // nothing received yet, so let's wait
                        poller.poll(timeout.toLong())

                        if (poller.pollin(SERVER_INDEX)) {
                            val payload = ZMsg.recvMsg(sockets[SERVER_INDEX]).transformZMsg(kryo)
                            if (payload != null) {
                                payloadToZMsg(payload, kryo).send(sockets[ORDER_INDEX])
                            }
                            return
                        } else {
                            logger.debug("Did not receive a server response in time, or another order needs to be executed")
                        }
                    }

                    // send back an empty response
                    ZMsg.newStringMsg(ORDERS.EMPTY.name).send(sockets[ORDER_INDEX])
                    return  // no need to do final valid check as we have already replied
                } else if (valid && ORDERS.SEND.name == orderType) {
                    logger.trace("ORDER = Send to Broker")

                    //the zMsg should consist of two parts only as others are popped
                    val payload = msg.transformZMsg(kryo)

                    if (payload != null) {
                        payloadToZMsg(payload, kryo).send(sockets[SERVER_INDEX])
                        ZMsg.newStringMsg(ORDERS.CONFIRM.name).send(sockets[ORDER_INDEX])
                    } else {
                        logger.warn("Cannot run send as given message is incompatible")
                        valid = false
                    }
                }

                if (!valid) {
                    // send order response if not already done
                    ZMsg.newStringMsg(ORDERS.FAIL.name).send(sockets[ORDER_INDEX])
                }
            }
            else -> logger.error("Cannot process message for socket at index {}, as this index is not known.",
                    socketIndex)
        }

    }

    override fun utilizationCalculated(utilization: Double) {
        logger.info("Current Utilization is {}%", utilization)
    }

    override fun shutdownCompleted() {
        logger.info("Shut down ZMQProcess_SimpleClient {}", identity)
    }

}
