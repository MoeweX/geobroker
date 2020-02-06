package de.hasenburg.geobroker.commons.model.message

import kotlinx.serialization.Serializable

/**
 * [topic] - individual topic levels must be separated by /, e.g., this/is/a/six/level/topic
 */
@Serializable
data class Topic(val topic: String) {

    val levelSpecifiers = topic.split("/").toTypedArray()
    val numberOfLevels = levelSpecifiers.size

    fun getLevelSpecifier(levelIndex: Int): String {
        if (levelIndex >= numberOfLevels) {
            throw RuntimeException("Look what you did, you killed the server by not checking the number of levels beforehand!")
        }
        return levelSpecifiers[levelIndex]
    }
}