package de.hasenburg.geobroker.server.distribution;

import de.hasenburg.geobroker.commons.model.BrokerInfo;
import de.hasenburg.geobroker.commons.model.JSONable;
import de.hasenburg.geobroker.commons.model.spatial.Geofence;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class BrokerAreaManager {

	private static final Logger logger = LogManager.getLogger();

	private final String ownBrokerId;
	private BrokerArea ownArea;
	private List<BrokerArea> otherAreas = new ArrayList<>();

	public BrokerAreaManager(String ownBrokerId) {
		this.ownBrokerId = ownBrokerId;
	}

	public void setup_DefaultFile() {
		String json = "[]";
		InputStream is = BrokerAreaManager.class.getClassLoader().getResourceAsStream("defaultBrokerAreas.json");
		try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
			json = br.lines().collect(Collectors.joining(System.lineSeparator()));
		} catch (IOException | NullPointerException e) {
			logger.fatal("Could not read default broker area file", e);
			System.exit(1);
		}

		createFromJson(json);
	}

	public void updateOwnBrokerArea(BrokerArea ownArea) {
		this.ownArea = ownArea;
	}

	public boolean checkIfResponsibleForClientLocation(Location clientLocation) {
		return ownArea.ContainsLocation(clientLocation);
	}

	public boolean checkOurAreaForMessageGeofence(Geofence messageGeofence) {
		return ownArea.intersects(messageGeofence);
	}

	public @Nullable BrokerInfo getOtherBrokerForClientLocation(Location clientLocation) {
		for (BrokerArea area : otherAreas) {
			if (area.ContainsLocation(clientLocation)) {
				return area.getResponsibleBroker();
			}
		}
		return null;
	}

	public List<BrokerInfo> getOtherBrokersForMessageGeofence(Geofence messageGeofence) {
		List<BrokerInfo> otherBrokers = new ArrayList<>();
		for (BrokerArea area : otherAreas) {
			if (area.intersects(messageGeofence)) {
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
			JSONObject object = jsonArray.getJSONObject(i);
			Optional<BrokerArea> areaO = JSONable.fromJSON(object.toString(), BrokerArea.class);
			areaO.ifPresent(area -> {
				if (area.CheckResponsibleBroker(ownBrokerId)) {
					ownArea = area;
				} else {
					otherAreas.add(area);
				}
			});
		}
	}

}
