package de.hasenburg.geofencebroker.model.storage;

import de.hasenburg.geofencebroker.model.exceptions.RuntimeStorageException;
import de.hasenburg.geofencebroker.model.spatial.Geofence;
import de.hasenburg.geofencebroker.model.spatial.Location;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Raster {

	private static final Logger logger = LogManager.getLogger();

	private final ConcurrentHashMap<Location, RasterEntry> rasterEntries = new ConcurrentHashMap<>();

	protected final int granularity;
	protected final double degreeStep; // = 1 / granularity

	/**
	 * Creates a new Raster. The index of each raster entry is the location of the entry's south west corner
	 *
	 * The raster always contains the whole world. Limitations to allowed subscriptions, e.g., the broker should only
	 * accept subscriptions from Europe, have to be made on another level
	 *
	 * The granularity is used to calculate the size of each {@link RasterEntry} in degrees. The size equals 1 degree /
	 * granularity
	 *
	 * @param granularity - must be >= 1
	 * @throws RuntimeStorageException if granularity < 1
	 */
	protected Raster(int granularity) {
		if (granularity < 1) {
			throw new RuntimeStorageException("Granularity must be >= 1, is " + granularity);
		}

		this.granularity = granularity;
		this.degreeStep = 1.0 / granularity;
		logger.debug("Raster created, granularity = {}, degree step = {}", granularity, degreeStep);
	}

	/*****************************************************************
	 * Subscribe/Unsubscribe Operations
	 ****************************************************************/

	/**
	 * Adds a subscriptionId to the fitting {@link RasterEntry}s
	 *
	 * @param geofence - the geofence used to calculate the fitting {@link RasterEntry}
	 * @param subscriptionId - the subscriptionId to be added
	 */
	protected void putSubscriptionIdIntoRasterEntries(Geofence geofence,
													  ImmutablePair<String, Integer> subscriptionId) {
		// get all RasterEntries to which the id should be added
		List<RasterEntry> viableRasterEntries = calculateIndexLocations(geofence);

		// add the subscriptionId to the RasterEntries
		for (RasterEntry viableRasterEntry : viableRasterEntries) {
			viableRasterEntry.putSubscriptionId(subscriptionId);
		}
	}

	/**
	 * Removes a subscriptionId from the fitting {@link RasterEntry}s. Probably used for unsubscribe operations.
	 *
	 * @param geofence - the geofence used to calculate the fitting {@link RasterEntry}
	 * @param subscriptionId - the subscription id to be removed
	 */
	protected void removeSubscriptionIdFromRasterEntries(Geofence geofence,
														 ImmutablePair<String, Integer> subscriptionId) {
		// get all RasterEntries from which the id should be removed
		List<RasterEntry> viableRasterEntries = calculateIndexLocations(geofence);

		// remove the subscriptionId from the RasterEntries
		for (RasterEntry viableRasterEntry : viableRasterEntries) {
			viableRasterEntry.removeSubscriptionId(subscriptionId);
		}
	}

	/**
	 * Removes a subscriptionId from a single {@link RasterEntry}. Probably used when a subscription is renewed with a
	 * new geofence and outdated subscription ids need to be removed.
	 *
	 * @param index - index of {@link RasterEntry} from which the subscription id should be removed
	 * @param subscriptionId - the subscription id to be removed
	 */
	protected void removeSubscriptionIdFromRasterEntry(Location index, ImmutablePair<String, Integer> subscriptionId) {
		RasterEntry re = rasterEntries.get(index);
		re.removeSubscriptionId(subscriptionId);
	}

	/*****************************************************************
	 * Process Published Message Operations
	 ****************************************************************/

	/**
	 * Returns all subscriptionIds that are in the fitting {@link RasterEntry}. In case no subscription ids exist, the
	 * returned Map is empty.
	 *
	 * @param location - the location that determines which {@link RasterEntry} fits
	 * @return a Map containing all fitting subscriptionIds; client -> set of subscription ids
	 */
	protected Map<String, Set<ImmutablePair<String, Integer>>> getSubscriptionIdsForPublisherLocation(
			Location location) {
		Location index = calculateIndexLocation(location);
		RasterEntry re = rasterEntries.get(index);
		if (re != null) {
			return re.getAllSubscriptionIds();
		}
		return Collections.unmodifiableMap(new HashMap<>());
	}

	/*****************************************************************
	 * Getters
	 ****************************************************************/

	protected int getNumberOfExistingRasterEntries() {
		return rasterEntries.size();
	}

	/*****************************************************************
	 * Private methods
	 ****************************************************************/

	/**
	 * Calculates at what index the corresponding {@link RasterEntry} is stored
	 *
	 * @param location - the location
	 * @return - the index
	 */
	private Location calculateIndexLocation(Location location) {
		double latIndex = Math.floor(location.getLat() * granularity) / granularity;
		double lonIndex = Math.floor(location.getLon() * granularity) / granularity;

		return new Location(latIndex, lonIndex);
	}

	/**
	 * Calculates with which {@link RasterEntry} the given geofence intersects with.
	 *
	 * @param geofence - the geofence
	 * @return - a list of {@link RasterEntry}s
	 */
	private List<RasterEntry> calculateIndexLocations(Geofence geofence) {
		// get north east and south west indices
		Location northEastIndex = calculateIndexLocation(geofence.getBoundingBoxNorthEast());
		Location southWestIndex = calculateIndexLocation(geofence.getBoundingBoxSouthWest());

		// get raster entries that have to be checked for intersection
		List<RasterEntry> rasterEntriesToCheckForIntersection = new ArrayList<>();
		for (double lat = southWestIndex.getLat(); lat <= northEastIndex.getLat(); lat += degreeStep) {
			for (double lon = southWestIndex.getLon(); lon <= northEastIndex.getLon(); lon += degreeStep) {
				lat = Math.round(lat * granularity) / (double) granularity;
				lon = Math.round(lon * granularity) / (double) granularity;
				Location index = new Location(lat, lon);
				RasterEntry re = rasterEntries.computeIfAbsent(index, k -> new RasterEntry(index, degreeStep));
				rasterEntriesToCheckForIntersection.add(re);
			}
		}

		// if geofence is a rectangle, we can collect the indices
		if (geofence.isRectangle()) {
			return rasterEntriesToCheckForIntersection;
		}

		// remove raster entries whose box is disjoint with the actual geofence
		rasterEntriesToCheckForIntersection.removeIf(re -> re.getRasterEntryBox().disjoint(geofence));

		// return
		return rasterEntriesToCheckForIntersection;
	}

	/*****************************************************************
	 * No Geo-Context (used to calculate Overhead)
	 *
	 * In order to use the storage without geo-context information,
	 * the two private methods above need to be commented out.
	 ****************************************************************/

//	private final Location index = new Location(0.0, 0.0);
//
//	private Location calculateIndexLocation(Location location) {
//		return index;
//	}
//
//	/**
//	 * Calculates with which {@link RasterEntry} the given geofence intersects with.
//	 *
//	 * @param geofence - the geofence
//	 * @return - a list of {@link RasterEntry}s
//	 */
//	private List<RasterEntry> calculateIndexLocations(Geofence geofence) {
//		List<RasterEntry> list = new ArrayList<>();
//		list.add(rasterEntries.computeIfAbsent(index, k -> new RasterEntry(index, degreeStep))); // only one entry
//		return list;
//	}

}
