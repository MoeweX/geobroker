package de.hasenburg.geolife;

import de.hasenburg.geobroker.commons.model.spatial.Location;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Route {

	private static final Logger logger = LogManager.getLogger();
	private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss");
	private List<ImmutablePair<Location, Integer>> visitedLocations = new ArrayList<>();

	/**
	 * Add a location to the visited locations list.
	 *
	 * @param location - to be added location
	 * @param requiredTravelTime - travel time needed to reach the location (in seconds)
	 */
	private void addLocation(Location location, Integer requiredTravelTime) {
		visitedLocations.add(ImmutablePair.of(location, requiredTravelTime));
	}

	public List<ImmutablePair<Location, Integer>> getVisitedLocations() {
		return this.visitedLocations;
	}

	public static Route createRoute(List<String> routeFileLines) {
		Route r = new Route();
		Date lastDate = null;
		Date newDate = null;
		for (String line : routeFileLines) {
			if (!isValidLine(line)) {
				continue;
			}

			String[] split = line.split(",");
			double lat = Double.parseDouble(split[0]);
			double lon = Double.parseDouble(split[1]);
			Location l = new Location(lat, lon);
			try {
				newDate = format.parse(split[5] + split[6]);
			} catch (ParseException e) {
				logger.fatal("Could not parse date for line {}");
				System.exit(1);
			}

			if (lastDate == null) {
				// this is the starting location
				r.addLocation(l, 0);
			} else {
				r.addLocation(l, (int) (newDate.getTime() - lastDate.getTime()) / 1000);
			}

			lastDate = newDate;
			newDate = null;
		}
		logger.trace("Created route {}", r);
		return r;
	}

	private static boolean isValidLine(String line) {
		if (line.split(",").length == 7) {
			return true;
		}

		logger.trace("Line is not valid: {}", line);
		return false;
	}

	@Override
	public String toString() {
		return "Route{" + "visitedLocations=" + visitedLocations + '}';
	}
}
