package de.hasenburg.geobroker.server.storage

import de.hasenburg.geobroker.commons.model.message.Topic
import org.apache.logging.log4j.LogManager
import java.util.concurrent.ConcurrentHashMap

private val logger = LogManager.getLogger()

const val SINGLE_LEVEL_WILDCARD: String = "+"
const val MULTI_LEVEL_WILDCARD: String = "#"

/**
 * A [TopicLevel] is a single part of a complete [Topic]. For example, the topic a/b/c has the three
 * topic levels a, b, and c. Topic levels can also be single level wildcards [SINGLE_LEVEL_WILDCARD] or
 * multilevel wildcards [MULTI_LEVEL_WILDCARD].
 */
class TopicLevel(val levelSpecifier: String, granularity: Int) {

    val raster: Raster
    // levelSpecifier -> TopicLevel
    private val children = ConcurrentHashMap<String, TopicLevel>()

    init {
        raster = Raster(granularity)
    }

    /*****************************************************************
     * Subscribe/Unsubscribe Operations
     ****************************************************************/

    /**
     * Gets an already existing child for the given level specifiers. A minimum of one specifier must be provided. If at
     * one point none exist yet, it and all subsequent ones will be created.
     *
     * @param levelSpecifiers - the given level specifiers
     * @return the child
     */
    fun getOrCreateChildren(vararg levelSpecifiers: String): TopicLevel {
        var currentChild = this
        for (specifier in levelSpecifiers) {
            currentChild = currentChild.children.computeIfAbsent(specifier) { k ->
                TopicLevel(specifier, raster.granularity)
            }
        }

        return currentChild
    }

    /**
     * Gets an already existing child for the given level specifiers. A minimum of one specifier must be provided. If at
     * one point none exist yet, this method returns null
     *
     * @param levelSpecifiers - the given level specifiers
     * @return the child or null
     */
    fun getChildren(vararg levelSpecifiers: String): TopicLevel? {
        var currentChild: TopicLevel? = this
        for (specifier in levelSpecifiers) {
            currentChild = currentChild?.children?.get(specifier) ?: return null
        }
        return currentChild
    }

    /*****************************************************************
     * Process Published Message Operations
     ****************************************************************/

    fun getDirectChild(levelSpecifier: String): TopicLevel? {
        return children[levelSpecifier]
    }

    fun getAllDirectChildren(): Collection<TopicLevel> {
        return children.values
    }

}
