package de.hasenburg.geobroker.commons.model.spatial

import de.hasenburg.geobroker.commons.exceptions.RuntimeShapeException
import de.hasenburg.geobroker.commons.model.spatial.SpatialContextK.GEO
import kotlinx.serialization.*
import kotlinx.serialization.internal.StringDescriptor
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration
import org.apache.logging.log4j.LogManager
import org.locationtech.spatial4j.shape.Rectangle
import org.locationtech.spatial4j.shape.Shape
import org.locationtech.spatial4j.shape.SpatialRelation

private val logger = LogManager.getLogger()

@Serializable
class Geofence(@Serializable(with = ShapeWKTSerializer::class) @SerialName("wkt") private val shape: Shape) {

    val center: Location
        get() = Location(shape.center)

    fun isRectangle(): Boolean {
        return GEO.shapeFactory.getGeometryFrom(shape).isRectangle
    }

    /*****************************************************************
     * Relationships
     ****************************************************************/

    /**
     * Returns true if this geofence contains the given [location].
     * Returns false if not or [location] == null.
     */
    fun contains(location: Location?): Boolean {
        return if (location != null) {
            shape.relate(location.point) == SpatialRelation.CONTAINS
        } else {
            false
        }
    }

    /**
     * For us, intersects is an "intersection" but also something more specific such as "contains" or within.
     */
    fun intersects(geofence: Geofence): Boolean {
        val sr = shape.relate(geofence.shape)
        return sr == SpatialRelation.INTERSECTS || sr == SpatialRelation.CONTAINS || sr == SpatialRelation.WITHIN
    }

    fun disjoint(geofence: Geofence): Boolean {
        return shape.relate(geofence.shape) == SpatialRelation.DISJOINT
    }

    /*****************************************************************
     * Bounding Box
     ****************************************************************/

    val boundingBox: Rectangle
        get() = shape.boundingBox

    val boundingBoxNorthWest: Location
        get() = Location(boundingBox.maxY, boundingBox.minX)
    val boundingBoxNorthEast: Location
        get() = Location(boundingBox.maxY, boundingBox.maxX)
    val boundingBoxSouthEast: Location
        get() = Location(boundingBox.minY, boundingBox.maxX)
    val boundingBoxSouthWest: Location
        get() = Location(boundingBox.minY, boundingBox.minX)

    /**
     * See [Rectangle.getHeight], is the latitude distance in degree
     */
    val boundingBoxLatDistanceInDegree: Double
        get() = boundingBox.height

    /**
     * See [Rectangle.getWidth], is the longitude distance in degree
     */
    val boundingBoxLonDistanceInDegree: Double
        get() = boundingBox.width

    /*****************************************************************
     * Others
     ****************************************************************/

    companion object {

        fun circle(location: Location, radiusDegree: Double): Geofence {
            return Geofence(GEO.shapeFactory.circle(location.point, radiusDegree))
        }

        fun rectangle(southWest: Location, northEast: Location): Geofence {
            return Geofence(GEO.shapeFactory.rect(southWest.point, northEast.point))
        }

        fun world(): Geofence {
            return Geofence(GEO.worldBounds)
        }

        fun fromWkt(wkt: String): Geofence {
            val reader = GEO.formats.wktReader as CustomWKTReader
            return Geofence(reader.parse(wkt) as Shape)
        }

        /**
         * Creates a new geofence based on the supplied locations
         *
         * @param surroundingLocations - the locations that surround the geofence
         * @return a new geofence
         * @throws RuntimeShapeException if less than three [surroundingLocations]
         */
        fun polygon(surroundingLocations: List<Location>): Geofence {
            if (surroundingLocations.size < 3) {
                throw RuntimeShapeException("A geofence needs at least 3 locations")
            }
            val polygonBuilder = GEO.shapeFactory.polygon()
            for (location in surroundingLocations) {
                polygonBuilder.pointLatLon(location.lat, location.lon)
            }
            // close polygon
            polygonBuilder.pointLatLon(surroundingLocations[0].lat, surroundingLocations[0].lon)
            return Geofence(polygonBuilder.build())
        }
    }

    override fun toString(): String {
        return GEO.formats.wktWriter.toString(shape)
    }

    /*****************************************************************
     * Generated methods
     ****************************************************************/

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Geofence

        if (shape != other.shape) return false

        return true
    }

    override fun hashCode(): Int {
        return shape.hashCode()
    }

}

/*****************************************************************
 * Json Serialization
 ****************************************************************/

fun Geofence.toJson(): String {
    return Json(JsonConfiguration.Stable).stringify(Geofence.serializer(), this)
}

/**
 * @throws [kotlinx.serialization.json.JsonDecodingException]
 */
fun String.toGeofence(): Geofence {
    return Json(JsonConfiguration.Stable).parse(Geofence.serializer(), this)
}

object ShapeWKTSerializer : KSerializer<Shape> {

    override val descriptor: SerialDescriptor = StringDescriptor

    override fun serialize(encoder: Encoder, obj: Shape) {
        encoder.encodeString(GEO.formats.wktWriter.toString(obj))
    }

    override fun deserialize(decoder: Decoder): Shape {
        val reader = GEO.formats.wktReader as CustomWKTReader
        return reader.parse(decoder.decodeString()) as Shape
    }
}
