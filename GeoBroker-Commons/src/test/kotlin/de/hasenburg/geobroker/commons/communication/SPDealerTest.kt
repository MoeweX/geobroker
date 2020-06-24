package de.hasenburg.geobroker.commons.communication

import de.hasenburg.geobroker.commons.sleepNoLog
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.logging.log4j.LogManager
import org.junit.After
import org.junit.Before
import org.junit.Test

import org.junit.Assert.*
import org.zeromq.SocketType
import org.zeromq.ZContext
import org.zeromq.ZMsg

class SPDealerTest {

    private val logger = LogManager.getLogger()
    private lateinit var zContext: ZContext

    @Before
    fun setUp() {
        zContext = ZContext(1)
    }

    @After
    fun tearDown() {
        zContext.destroy()
    }

    @Test
    fun shutdown() {
        val spDealer = SPDealer()
        assertTrue(spDealer.isActive)
        spDealer.shutdown()
        assertFalse(spDealer.isActive)
    }

    @Test
    fun tearUpTearDown() {
        val spDealer = SPDealer()
        spDealer.shutdown()
    }

    /**
     * When this tests blocks, sending or receiving does not work.
     */
    @Test
    fun sendAndReceive() {
        val content = "content"
        val names = listOf("client1", "client2")

        runBlocking {
            val spDealer = SPDealer()

            launch(Dispatchers.Default) {
                val socket = zContext.createSocket(SocketType.ROUTER).also {
                    it.bind("tcp://${spDealer.ip}:${spDealer.port}")
                }

                repeat(4) {
                    logger.info("Waiting for message $it from SPDealer")
                    val msg = ZMsg.recvMsg(socket)
                    val sender = msg.popString()
                    assertTrue(sender in names)
                    assertEquals(content, msg.popString())
                    assertTrue(msg.isEmpty())
                    ZMsg.newStringMsg(sender, "ok").send(socket)
                    logger.info("Responded to $sender with ok")
                }
            }

            repeat(2) {
                for (name in names) {
                    val msg = ZMsg.newStringMsg(name, content)
                    spDealer.toSent.send(msg)
                }
            }

            // check wasSend
            repeat(4) {
                // these might be in a weird order
                logger.info("We sent message ${spDealer.wasSent.receive()}")
                logger.info("We got response ${spDealer.wasReceived.receive()}")
            }
            assertNull(spDealer.wasSent.poll())
            assertNull(spDealer.wasReceived.poll())

            assertEquals(names.size, spDealer.numberOfPersonalities)
        }


    }
}