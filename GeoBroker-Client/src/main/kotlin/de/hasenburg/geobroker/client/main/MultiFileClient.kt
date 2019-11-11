package de.hasenburg.geobroker.client.main

import de.hasenburg.geobroker.commons.communication.SPDealer
import de.hasenburg.geobroker.commons.communication.ZMsgTP
import de.hasenburg.geobroker.commons.model.KryoSerializer
import de.hasenburg.geobroker.commons.model.message.*
import de.hasenburg.geobroker.commons.model.message.Payload.PINGREQPayload
import de.hasenburg.geobroker.commons.model.message.Payload.SUBSCRIBEPayload
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.apache.logging.log4j.LogManager
import org.zeromq.ZMsg
import java.io.File
import java.lang.NumberFormatException
import kotlin.math.floor

private val logger = LogManager.getLogger()

fun main(args: Array<String>) {
    val dir: File = if (args.size == 1) {
        File(args[0])
    } else {
        File("GeoBroker-Client/src/main/resources/multifile")
    }
    logger.info("Looking for files at ${dir.absolutePath}")

    val spd = SPDealer()
    val fileList = dir.listFiles()?.filter { f -> f.extension == "csv" } ?: emptyList()

    logger.info("Using ${fileList.size} files")

    runBlocking {
        launch(Dispatchers.IO) {
            writeChannelInputToFileBlocking(spd.wasSent, File(dir.absolutePath + "/wasSent.txt"))
        }
        launch(Dispatchers.IO) {
            writeChannelInputToFileBlocking(spd.wasReceived, File(dir.absolutePath + "/wasReceived.txt"))
        }

        launch(Dispatchers.Default) {
            startFileProcessingBlocking(System.currentTimeMillis() + 1000, spd.toSent, fileList)
            // we are done
            spd.shutdown()
        }
    }

    logger.info("File processing completed.")
}

suspend fun writeChannelInputToFileBlocking(
        channel: Channel<ZMsgTP>,
        output: File) = coroutineScope {

    val kryo = KryoSerializer()
    logger.info("Writing channel content to $output")
    val writer = output.bufferedWriter()
    writer.write("timestamp;msg\n")

    for (zMsgTP in channel) {
        writer.write("${zMsgTP.timestamp}\t${zMsgTP.msg.transformZMsgWithId(kryo).toString().replace("+", "")}\n")
    } // ends when channel is closed
    writer.close()
    logger.info("Channel was closed for file $output, shutting down")
}

suspend fun startFileProcessingBlocking(startTime: Long, toSent: Channel<ZMsg>,
                                        fileList: List<File>) = coroutineScope {

    // every file is processed in its own coroutine
    for (file in fileList) {
        launch { processFile(startTime, toSent, file) }
    }

    // the coroutineScope only returns when the processFile coroutines end
}

private suspend fun processFile(startTime: Long, toSent: Channel<ZMsg>, file: File) {
    val clientId = file.nameWithoutExtension
    val lines = file.readLines()
    val kryo = KryoSerializer()


    // line counting
    var previousPercent = 0.0f

    // send a connect message
    toSent.send(payloadToZMsg(Payload.CONNECTPayload(Location.undefined()), kryo, clientId))
    logger.debug("[$clientId] Sent connect")

    for ((i, line) in lines.withIndex()) {

        if (i == 0) {
            continue; // header
        }

        val percent = floor(i * 100f / lines.size).toFloat()
        // Compute your percentage.
        if (percent != previousPercent) {
            // Output if different from the last time.
            logger.debug("[$clientId] Processed ${percent.toInt()}% of all lines.")

        }
        // Update the percentage.
        previousPercent = percent

        val split = line.split(";")
        try {
            val timestamp = split[0].toLong()
            while (System.currentTimeMillis() < (startTime + timestamp)) {
                delay(1)
            }

            // time reached
            val behindSchedule = System.currentTimeMillis() - (startTime + timestamp)
            if (behindSchedule > 10) {
                logger.warn("[$clientId] We are ${behindSchedule}ms behind schedule with ${split[3]}!")
            }
            // send if valid line
            createZMsg(clientId, i, split, kryo)?.let { toSent.send(it) } ?: logger.warn("Empty ZMsg for $line")
        } catch (e: NumberFormatException) {
            logger.warn("$line is not a valid line")
        }
    }

    // disconnect
    delay(5000)
    toSent.send(payloadToZMsg(Payload.DISCONNECTPayload(ReasonCode.NormalDisconnection), kryo, clientId))
    logger.debug("[$clientId] Sent disconnect")
    delay(5000)
}

private fun createZMsg(clientId: String, messageNumber: Int, split: List<String>, kryo: KryoSerializer): ZMsg? {
    try {
        val msg: ZMsg

        val messageType = split[3]
        val lat = split[1].toDouble()
        val lon = split[2].toDouble()
        val topic = split[4]
        val geofence = split[5]

        when (messageType) {
            "ping" -> {
                logger.trace("Sending ping message")
                msg = payloadToZMsg(PINGREQPayload(Location(lat, lon)), kryo, clientId)
            }
            "subscribe" -> {
                logger.trace("Sending subscribe message")
                msg = payloadToZMsg(SUBSCRIBEPayload(Topic(topic), Geofence(geofence)), kryo, clientId)
            }
            "publish" -> {
                logger.trace("Sending publish message")
                msg = payloadToZMsg(Payload.PUBLISHPayload(Topic(topic),
                        Geofence(geofence),
                        generatePayload(clientId,
                                messageNumber,
                                topic,
                                Integer.parseInt(split[6]))), kryo, clientId)
            }
            else -> return null
        }
        return msg
    } catch (e: Exception) {
        logger.warn("Invalid line, discarding it", e)
        return null
    }
}

private fun generatePayload(thingId: String, tupleNr: Int, topic: String, payloadSize: Int): String {
    val gp = thingId + "_" + tupleNr + "_" + topic + "_"
    val builder = StringBuilder(gp)
    for (i in gp.toByteArray().size..payloadSize) {
        builder.append("+")
    }
    return builder.toString()
}