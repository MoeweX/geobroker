package de.hasenburg.geobroker.server.distribution;

import de.hasenburg.geobroker.commons.model.BrokerArea;
import de.hasenburg.geobroker.commons.model.BrokerInfo;
import de.hasenburg.geobroker.commons.model.spatial.Geofence;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BrokerAreaManager {

	private static final Logger logger = LogManager.getLogger();

	private final String ownBrokerId;
	private BrokerArea ownArea;
	private List<BrokerArea> otherAreas = new ArrayList<>();

	public BrokerAreaManager(String ownBrokerId) {
		this.ownBrokerId = ownBrokerId;
	}

	public void readFromFile(String filepath) {
		String json = "[]";
		File f = new File(filepath);
		try (BufferedReader br = new BufferedReader(new FileReader(f))) {
			json = br.lines().collect(Collectors.joining(System.lineSeparator()));
		} catch (IOException | NullPointerException e) {
			logger.fatal("Could not read broker area file from {}", f.getAbsolutePath(), e);
			System.exit(1);
		}

		createFromJson(json);
	}

	public void updateOwnBrokerArea(BrokerArea ownArea) {
		this.ownArea = ownArea;
	}

	public boolean checkIfOurAreaContainsLocation(Location clientLocation) {
		return ownArea.ContainsLocation(clientLocation);
	}

	public boolean checkOurAreaForGeofenceIntersection(Geofence messageGeofence) {
		return ownArea.intersects(messageGeofence);
	}

	public List<BrokerInfo> getAllBrokers() {
		return otherAreas.stream().map(area -> area.getResponsibleBroker()).collect(Collectors.toList());
	}

	public @Nullable BrokerInfo getOtherBrokerContainingLocation(Location clientLocation) {
		for (BrokerArea area : otherAreas) {
			if (area.ContainsLocation(clientLocation)) {
				return area.getResponsibleBroker();
			}
		}
		return null;
	}

	public List<BrokerInfo> getOtherBrokersIntersectingWithGeofence(Geofence geofence) {
		List<BrokerInfo> otherBrokers = new ArrayList<>();
		for (BrokerArea area : otherAreas) {
			if (area.intersects(geofence)) {
				otherBrokers.add(area.getResponsibleBroker());
			}
		}
		return otherBrokers;
	}

	public List<BrokerInfo> getOtherBrokerInfo() {
		return otherAreas.stream().map(BrokerArea::getResponsibleBroker).collect(Collectors.toList());
	}

	public String getOwnBrokerId() {
		return this.ownBrokerId;
	}

	public BrokerInfo getOwnBrokerInfo() {
		return ownArea.getResponsibleBroker();
	}

	/*****************************************************************
	 * Helper Methods
	 ****************************************************************/

	void createFromJson(String json) {
		JSONArray jsonArray = new JSONArray(json);
		for (int i = 0; i < jsonArray.length(); i++) {
			try {
				JSONObject object = jsonArray.getJSONObject(i);
				JSONObject responsibleBroker = object.getJSONObject("responsibleBroker");
				JSONObject coveredArea = object.getJSONObject("coveredArea");
				String ip = responsibleBroker.getString("ip");
				String brokerId = responsibleBroker.getString("brokerId");
				int port = responsibleBroker.getInt("port");
				String WKT = coveredArea.getString("WKT");
				BrokerArea area = new BrokerArea(new BrokerInfo(brokerId, ip, port), new Geofence(WKT));
				if (area.CheckResponsibleBroker(ownBrokerId)) {
					ownArea = area;
				} else {
					otherAreas.add(area);
				}
			} catch (JSONException | ParseException ex) {
				logger.fatal("Couldn't parse the BrokerInfo", ex);
				System.exit(1);
			}
		}
	}

}
