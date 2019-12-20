package de.hasenburg.geobroker.commons.model.spatial

import de.hasenburg.geobroker.commons.exceptions.RuntimeShapeException
import de.hasenburg.geobroker.commons.model.spatial.LocationWrapper.LocationK
import de.hasenburg.geobroker.commons.model.spatial.SpatialContextK.GEO
import kotlinx.serialization.*
import kotlinx.serialization.internal.StringDescriptor
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration
import org.apache.logging.log4j.LogManager
import org.locationtech.spatial4j.shape.Rectangle
import org.locationtech.spatial4j.shape.Shape
import org.locationtech.spatial4j.shape.SpatialRelation
import kotlin.system.measureTimeMillis

private val logger = LogManager.getLogger()

@Serializable
class GeofenceK(@Serializable(with = ShapeWKTSerializer::class) @SerialName("wkt") private val shape: Shape) {

    fun isRectangle(): Boolean {
        return GEO.shapeFactory.getGeometryFrom(shape).isRectangle
    }

    /*****************************************************************
     * Relationships
     ****************************************************************/

    fun contains(location: LocationK): Boolean {
        return shape.relate(location.point) == SpatialRelation.CONTAINS
    }

    /**
     * For us, intersects is an "intersection" but also something more specific such as "contains" or within.
     */
    fun intersects(geofence: GeofenceK): Boolean {
        val sr = shape.relate(geofence.shape)
        return sr == SpatialRelation.INTERSECTS || sr == SpatialRelation.CONTAINS || sr == SpatialRelation.WITHIN
    }

    fun disjoint(geofence: GeofenceK): Boolean {
        return shape.relate(geofence.shape) == SpatialRelation.DISJOINT
    }

    /*****************************************************************
     * Bounding Box
     ****************************************************************/

    val boundingBox: Rectangle
        get() = shape.boundingBox

    val boundingBoxNorthWest: LocationK
        get() = LocationK(boundingBox.maxY, boundingBox.minX)
    val boundingBoxNorthEast: LocationK
        get() = LocationK(boundingBox.maxY, boundingBox.maxX)
    val boundingBoxSouthEast: LocationK
        get() = LocationK(boundingBox.minY, boundingBox.maxX)
    val boundingBoxSouthWest: LocationK
        get() = LocationK(boundingBox.minY, boundingBox.minX)

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

        fun circle(location: LocationK, radiusDegree: Double): GeofenceK {
            return GeofenceK(GEO.shapeFactory.circle(location.point, radiusDegree))
        }

        fun rectangle(southWest: Location, northEast: Location): GeofenceK {
            return GeofenceK(GEO.shapeFactory.rect(southWest.point, northEast.point))
        }

        fun world(): GeofenceK {
            return GeofenceK(GEO.worldBounds)
        }

        /**
         * Creates a new geofence based on the supplied locations
         *
         * @param surroundingLocations - the locations that surround the geofence
         * @return a new geofence
         * @throws RuntimeShapeException if less than three [surroundingLocations]
         */
        fun polygon(surroundingLocations: List<Location>): GeofenceK {
            if (surroundingLocations.size < 3) {
                throw RuntimeShapeException("A geofence needs at least 3 locations")
            }
            val polygonBuilder = GEO.shapeFactory.polygon()
            for (location in surroundingLocations) {
                polygonBuilder.pointLatLon(location.lat, location.lon)
            }
            // close polygon
            polygonBuilder.pointLatLon(surroundingLocations[0].lat, surroundingLocations[0].lon)
            return GeofenceK(polygonBuilder.build())
        }
    }

    override fun toString(): String {
        return GEO.formats.wktWriter.toString(shape)
    }

}

/*****************************************************************
 * Json Serialization
 ****************************************************************/

fun GeofenceK.toJson(): String {
    return Json(JsonConfiguration.Stable).stringify(GeofenceK.serializer(), this)
}

fun String.toGeofence(): GeofenceK {
    return Json(JsonConfiguration.Stable).parse(GeofenceK.serializer(), this)
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

/*****************************************************************
 * Main to test
 ****************************************************************/

fun main() {
    val c1 = GeofenceK.circle(LocationK.random(), 10.0)
    logger.info("Geofence 1: $c1")
    val j = c1.toJson()
    logger.info("Geofence as Json: $j")
    val c2 = j.toGeofence()
    logger.info("Geofence 2: $c2")
    assert(c1 == c2)

    val paris = LocationK(48.86, 2.35)
    val berlin = LocationK(52.52, 13.40)
    val parisArea = GeofenceK.circle(paris, 3.0)
    val berlinArea = GeofenceK.circle(berlin, 3.0)

    logger.info("Paris area = {}", parisArea)
    logger.info("Berlin area = {}", berlinArea)
    logger.info("The areas intersect: {}", berlinArea.intersects(parisArea))

    val justIn = LocationK(45.87, 2.3)
    logger.info(parisArea.contains(justIn))

    logger.info("Contains check Benchmark")
    val world = GeofenceK.world()
    val amount: Long = 10000000

    logger.info("{} berlin in circle checks per ms", amount / measureTimeMillis {
        for (i in 0 until amount) {
            berlinArea.contains(berlin)
        }
    })

    logger.info("{} berlin out circle checks per ms", amount / measureTimeMillis {
        for (i in 0 until amount) {
            berlinArea.contains(paris)
        }
    })

    logger.info("{} world checks per ms", amount / measureTimeMillis {
        for (i in 0 until amount) {
            world.contains(berlin)
        }
    })
}