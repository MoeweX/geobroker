@file:JvmName("Utility")

package de.hasenburg.geobroker.commons

import de.hasenburg.geobroker.commons.model.JSONable
import de.hasenburg.geobroker.commons.model.message.*
import de.hasenburg.geobroker.commons.exceptions.CommunicatorException
import de.hasenburg.geobroker.commons.model.message.payloads.*
import de.hasenburg.geobroker.commons.model.message.payloads.BrokerForwardPublishPayload
import me.atrox.haikunator.Haikunator
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.Configuration
import org.apache.logging.log4j.core.config.LoggerConfig

import java.util.Random
import java.util.function.Supplier

private val logger = LogManager.getLogger()
private val r = Random()
private var haikunator: Haikunator = Haikunator().setRandom(r)

fun sleep(millis: Long, nanos: Int) {
    try {
        Thread.sleep(millis, nanos)
    } catch (e: InterruptedException) {
        Thread.currentThread().interrupt()
        logger.error("Interrupted my sleep :S -> interrupting!", e)
    }

}

fun sleepNoLog(millis: Long, nanos: Int) {
    try {
        Thread.sleep(millis, nanos)
    } catch (e: InterruptedException) {
        Thread.currentThread().interrupt()
    }

}

/**
 * Returns true with the given chance.
 *
 * @param chance - the chance to return true (0 - 100)
 * @return true, if lucky
 */
fun getTrueWithChance(chance: Int): Boolean {
    @Suppress("NAME_SHADOWING") var chance = chance
    // normalize
    if (chance > 100) {
        chance = 100
    } else if (chance < 0) {
        chance = 0
    }
    val random = r.nextInt(100) + 1 // not 0
    return random <= chance
}

/**
 * @param bound - int, must be > 0
 * @return a random int between 0 (inclusive) and bound (exclusive)
 */
fun randomInt(bound: Int): Int {
    return r.nextInt(bound)
}

fun randomName(): String {
    return haikunator.haikunate()
}

fun randomName(r: Random): String {
    val h = Haikunator().setRandom(r)
    h.random = r
    return h.haikunate()
}

/**
 * Set the logger level of the given logger to the given level.
 *
 * @param logger - the logger
 * @param level - the level
 */
fun setLogLevel(logger: Logger, level: Level) {
    val ctx = LogManager.getContext(false) as LoggerContext
    val conf = ctx.configuration
    val loggerConfig = conf.getLoggerConfig(logger.name)
    loggerConfig.level = level
    ctx.updateLoggers(conf)
}

/**
 * Returns payload corresponding to the control packet type in the form of an abstract payload. Checks whether all
 * fields are not null.
 *
 * @throws CommunicatorException if control packet type is not supported or a field is null.
 */
@Throws(CommunicatorException::class)
fun buildPayloadFromString(s: String, controlPacketType: ControlPacketType): AbstractPayload {

    when (controlPacketType) {
        ControlPacketType.CONNACK -> {
            val connackPayload = JSONable.fromJSON<CONNACKPayload>(s, CONNACKPayload::class.java).orElseGet({ CONNACKPayload() })
            if (!connackPayload.nullField()) {
                return connackPayload
            }
        }
        ControlPacketType.CONNECT -> {
            val connectPayload = JSONable.fromJSON<CONNECTPayload>(s, CONNECTPayload::class.java).orElseGet({ CONNECTPayload() })
            if (!connectPayload.nullField()) {
                return connectPayload
            }
        }
        ControlPacketType.DISCONNECT -> {
            val disconnectPayload = JSONable.fromJSON<DISCONNECTPayload>(s, DISCONNECTPayload::class.java).orElseGet({ DISCONNECTPayload() })
            if (!disconnectPayload.nullField()) {
                return disconnectPayload
            }
        }
        ControlPacketType.PINGREQ -> {
            val pingreqPayload = JSONable.fromJSON<PINGREQPayload>(s, PINGREQPayload::class.java).orElseGet({ PINGREQPayload() })
            if (!pingreqPayload.nullField()) {
                return pingreqPayload
            }
        }
        ControlPacketType.PINGRESP -> {
            val pingrespPayload = JSONable.fromJSON<PINGRESPPayload>(s, PINGRESPPayload::class.java).orElseGet({ PINGRESPPayload() })
            if (!pingrespPayload.nullField()) {
                return pingrespPayload
            }
        }
        ControlPacketType.PUBACK -> {
            val pubackPayload = JSONable.fromJSON<PUBACKPayload>(s, PUBACKPayload::class.java).orElseGet({ PUBACKPayload() })
            if (!pubackPayload.nullField()) {
                return pubackPayload
            }
        }
        ControlPacketType.PUBLISH -> {
            val publishPayload = JSONable.fromJSON<PUBLISHPayload>(s, PUBLISHPayload::class.java).orElseGet({ PUBLISHPayload() })
            if (!publishPayload.nullField()) {
                return publishPayload
            }
        }
        ControlPacketType.SUBACK -> {
            val subackPayload = JSONable.fromJSON<SUBACKPayload>(s, SUBACKPayload::class.java).orElseGet({ SUBACKPayload() })
            if (!subackPayload.nullField()) {
                return subackPayload
            }
        }
        ControlPacketType.SUBSCRIBE -> {
            val subscribePayload = JSONable.fromJSON<SUBSCRIBEPayload>(s, SUBSCRIBEPayload::class.java).orElseGet({ SUBSCRIBEPayload() })
            if (!subscribePayload.nullField()) {
                return subscribePayload
            }
        }
        ControlPacketType.BrokerForwardPublish -> {
            val brokerForwardPublishPayload = JSONable
                    .fromJSON<BrokerForwardPublishPayload>(s, BrokerForwardPublishPayload::class.java).orElseGet({ BrokerForwardPublishPayload() })
            if (!brokerForwardPublishPayload.nullField()) {
                return brokerForwardPublishPayload
            }
        }
        else -> throw CommunicatorException("ControlPacketType ${controlPacketType.name} is not supported")
    }

    throw CommunicatorException("Some of the payloads fields that may not be null are null")
}

fun generateClientOrderBackendString(identity: String): String {
    return "inproc://$identity"
}

/**
 * Generates a string payload with the given size, but the minimum size is length(content) + 8.
 */
fun generatePayloadWithSize(payloadSize: Int, content: String): String {
    val stringBuilder = StringBuilder()
    stringBuilder.append(content).append("++++++++")
    for (i in 0 until payloadSize - 8 - content.length) {
        stringBuilder.append("a")
    }
    return stringBuilder.toString()
}
