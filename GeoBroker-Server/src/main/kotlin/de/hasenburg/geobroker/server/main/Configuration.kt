package de.hasenburg.geobroker.server.main

import com.moandjiezana.toml.Toml
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LoggerContext
import java.io.File
import kotlin.system.exitProcess

private val logger = LogManager.getLogger()

/**
 * @param brokerAreaFilePath - only has a meaning when [mode] == [Mode.disgb_subscriberMatching] or [Mode.disgb_publisherMatching].
 */
data class Configuration(
        // server
        val brokerId: String = "broker",
        val port: Int = 5559,
        val granularity: Int = 1,
        val messageProcessors: Int = 1,
        val logConfFile: File? = null,
        val prometheusPort: Int = -1,

        // server mode - general
        val mode: Mode = Mode.single
)

@Suppress("EnumEntryName")
enum class Mode {
    single,

    // other modes (not intended for production use)
    single_noGeo
}

fun getDefaultConfiguration(): Configuration {
    return Configuration()
}

fun readConfiguration(filePath: String): Configuration {
    try {
        val f = File(filePath)
        val toml = Toml().read(f) ?: throw Exception("Cannot read toml from file ${f.absolutePath}")
        return parseToml(toml)
    } catch (e: Exception) {
        logger.fatal("Could not read configuration", e)
    }
    exitProcess(1)
}

fun readInternalConfiguration(filePath: String): Configuration {
    try {
        val toml = Toml().read(Configuration::javaClass.javaClass.classLoader.getResourceAsStream(filePath))
                ?: throw Exception("Cannot read toml from internal file $filePath")
        return parseToml(toml)
    } catch (e: Exception) {
        logger.fatal("Could not load internal configuration", e);
    }
    exitProcess(1)
}

private fun parseToml(toml: Toml): Configuration { // server information
    val c = getDefaultConfiguration()

    // [server]
    val tomlServer: Toml? = toml.getTable("server")

    val brokerId = tomlServer?.getString("brokerId") ?: c.brokerId
    val port = tomlServer?.getInt("port") ?: c.port
    val granularity = tomlServer?.getInt("granularity") ?: c.granularity
    val messageProcessors = tomlServer?.getInt("messageProcessors") ?: c.messageProcessors
    val logConfFile = tomlServer?.getFile("logConfFile") ?: c.logConfFile
    val prometheusPort = tomlServer?.getInt("prometheusPort") ?: c.prometheusPort

    // [sever.mode]
    val tomlServerMode: Toml? = tomlServer?.getTable("mode")
    val mode = tomlServerMode?.getMode("name") ?: c.mode

    // update configuration if required
    logConfFile?.run {
        logger.info("Updating log config to {}", this.absolutePath)
        val context = LogManager.getContext(false) as LoggerContext
        // this will force a reconfiguration
        context.configLocation = this.toURI()
        logger.info("Configuration updated")
    }

    return Configuration(brokerId,
            port,
            granularity,
            messageProcessors,
            logConfFile,
            prometheusPort,
            mode)
}


/*****************************************************************
 * Extension Functions
 ****************************************************************/

fun Toml.getInt(key: String): Int? {
    getLong(key)?.run { return Math.toIntExact(this) } ?: return null
}

fun Toml.getMode(key: String): Mode? {
    getString(key)?.run { return Mode.valueOf(this) } ?: return null
}

fun Toml.getFile(key: String): File? {
    getString(key)?.run {
        val file = File(this)
        if (file.exists() && file.isFile) {
            return file
        }
    }
    return null
}