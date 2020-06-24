package de.hasenburg.geobroker.client.main

import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.InvalidArgumentException
import com.xenomachina.argparser.default
import com.xenomachina.argparser.mainBody
import de.hasenburg.geobroker.commons.communication.SPDealer
import de.hasenburg.geobroker.commons.communication.ZMsgTP
import de.hasenburg.geobroker.commons.model.message.*
import de.hasenburg.geobroker.commons.model.message.Payload.PINGREQPayload
import de.hasenburg.geobroker.commons.model.message.Payload.SUBSCRIBEPayload
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration
import me.tongfei.progressbar.ProgressBar
import me.tongfei.progressbar.ProgressBarStyle
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LoggerContext
import org.zeromq.ZMsg
import java.io.File
import kotlin.math.floor

private val logger = LogManager.getLogger()

/**
 * The MultiFileClient is a command line application.
 * Run it with the command line argument --help to see available options.
 *
 * The purpose of the MultiFileClient is to process files output by the IoTDSG (https://github.com/MoeweX/IoTDSG) and
 * send corresponding messages to the configured GeoBroker server.
 * Example files can be found in src/main/resources/multifile/
 *
 * The MultiFileClient writes all sent messages to wasSent.txt (including a timestamp), and all received messages to
 * wasReceived.txt (including a timestamp).
 */
fun main(args: Array<String>) {
    // configuration
    val conf = mainBody { ArgParser(args).parseInto(::ConfMultiFile) }

    conf.logConfFile?.let {
        // update log conf file
        val logConf = File(it)
        logger.info("Updating log config to {}", logConf.absolutePath)
        val context = LogManager.getContext(false) as LoggerContext

        // this will force a reconfiguration
        context.configLocation = logConf.toURI()
        logger.info("Configuration updated")
    }
    logger.info("Looking for files at ${conf.dir.absolutePath}")

    val spd = SPDealer(conf.serverIp, conf.serverPort, conf.socketHWM)
    val fileList = conf.dir.listFiles()?.filter { f -> f.extension == "csv" } ?: emptyList()
    logger.info("Using ${fileList.size} files")

    // Channels for the progress bar
    val pbAddToMax: Channel<Long> = Channel(Channel.UNLIMITED)
    val pbStep: Channel<Long> = Channel(Channel.UNLIMITED)

    runBlocking {
        launch(Dispatchers.IO) {
            writeChannelInputToFileBlocking(spd.wasSent, File(conf.dir.absolutePath + "/wasSent.txt"))
        }
        launch(Dispatchers.IO) {
            writeChannelInputToFileBlocking(spd.wasReceived, File(conf.dir.absolutePath + "/wasReceived.txt"))
        }
        launch(Dispatchers.Default) {
            displayProgressBarBlocking(pbAddToMax, pbStep)
        }
        launch(Dispatchers.Default) {
            startFileProcessingBlocking(System.currentTimeMillis() + 1000, spd.toSent, fileList, pbAddToMax, pbStep)
            // we are done. closes spd.toSent and spd.wasReceived
            spd.shutdown()
            // close the progress bar channels
            pbAddToMax.close()
            pbStep.close()

        }
    }

    logger.info("File processing completed.")
}

suspend fun writeChannelInputToFileBlocking(
        channel: ReceiveChannel<ZMsgTP>,
        output: File) = coroutineScope {

    val json = Json(JsonConfiguration.Stable)
    logger.info("Writing channel content to $output")
    val writer = output.bufferedWriter()
    writer.write("timestamp;msg\n")

    for (zMsgTP in channel) {
        writer.write("${zMsgTP.timestamp}\t${zMsgTP.msg.toPayloadAndId().toString().replace("+", "")}\n")
    } // ends when channel is closed

    writer.close()
    logger.info("Channel was closed for file $output, shutting down")
}

suspend fun startFileProcessingBlocking(startTime: Long, toSent: SendChannel<ZMsg>,
                                        fileList: List<File>, pbAddToMax: SendChannel<Long>,
                                        pbStep: SendChannel<Long>) = coroutineScope {

    // every file is processed in its own coroutine
    for (file in fileList) {
        launch { processFile(startTime, toSent, file, pbAddToMax, pbStep) }
    }

    // the coroutineScope only returns when the processFile coroutines end
}

suspend fun displayProgressBarBlocking(addToMax: ReceiveChannel<Long>, step: ReceiveChannel<Long>) {
    var maxSteps = 0L
    val pb = ProgressBar("Sent Messages", maxSteps, ProgressBarStyle.ASCII)
    for (message in step) {
        // First check if there are any new messages in the addToMax channel
        // This means that the maxHint will only be updated after a message has been sent
        if (!addToMax.isEmpty) {
            maxSteps += addToMax.receive()
            pb.maxHint(maxSteps)
        }
        pb.stepBy(message)
    }
    pb.close()
}

private suspend fun processFile(startTime: Long, toSent: SendChannel<ZMsg>, file: File, pbAddToMax: SendChannel<Long>,
                                pbStep: SendChannel<Long>) {
    val clientId = file.nameWithoutExtension
    val lines = file.readLines()
    val json = Json(JsonConfiguration.Stable)

    pbAddToMax.send((lines.size - 1).toLong())

    // send a connect message
    toSent.send(Payload.CONNECTPayload(null).toZMsg(json, clientId))
    logger.debug("[$clientId] Sent connect")

    for ((i, line) in lines.withIndex()) {

        if (i == 0) {
            continue // header
        }

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
            createZMsg(clientId, i, split, json)?.let { toSent.send(it) } ?: logger.warn("Empty ZMsg for $line")
        } catch (e: NumberFormatException) {
            logger.warn("$line is not a valid line")
        }

        // Update the progress bar
        pbStep.send(1L)
    }

    // disconnect
    delay(5000)
    toSent.send(Payload.DISCONNECTPayload(ReasonCode.NormalDisconnection).toZMsg(json, clientId))
    logger.debug("[$clientId] Sent disconnect")
    delay(5000)
}

private fun createZMsg(clientId: String, messageNumber: Int, split: List<String>, json: Json): ZMsg? {
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
                msg = PINGREQPayload(Location(lat, lon)).toZMsg(json, clientId)
            }
            "subscribe" -> {
                logger.trace("Sending subscribe message")
                msg = SUBSCRIBEPayload(Topic(topic), Geofence.fromWkt(geofence)).toZMsg(json, clientId)
            }
            "publish" -> {
                logger.trace("Sending publish message")
                msg = Payload.PUBLISHPayload(Topic(topic),
                        Geofence.fromWkt(geofence),
                        generatePayload(clientId,
                                messageNumber,
                                topic,
                                Integer.parseInt(split[6]))).toZMsg(json, clientId)
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

class ConfMultiFile(parser: ArgParser) {
    val dir by parser
        .storing("-d", "--dir", help = "local directory containing the input files") { File(this) }
        .default(File("GeoBroker-Client/src/main/resources/multifile"))
        .addValidator {
            if (!value.exists()) {
                throw InvalidArgumentException("Directory $value does not exist")
            }
            if (!value.isDirectory) {
                throw InvalidArgumentException("$value is not a directory")
            }
        }

    val serverIp by parser
        .storing("-i", "--ip-address", help = "ip address of the GeoBroker server")
        .default("localhost")

    val serverPort by parser
        .storing("-p", "--port", help = "port of the GeoBroker server") { this.toInt() }
        .default(5559)

    val logConfFile by parser
        .storing("-l", "--log-config", help = "config file for log4j")
        .default<String?>(null)

    val socketHWM by parser
        .storing("-w", "--high-watermark", help = "high water mark for each individual zmq socket") { this.toInt() }
        .default(1000)
}