package de.hasenburg.geobroker.commons.model.message.payloads;

import de.hasenburg.geobroker.commons.model.message.Topic;
import de.hasenburg.geobroker.commons.model.spatial.Geofence;

import java.util.Objects;

public class UNSUBSCRIBEPayload extends AbstractPayload {

	protected Topic topic;
	protected Geofence geofence;

	public UNSUBSCRIBEPayload() {

	}

	public UNSUBSCRIBEPayload(Topic topic, Geofence geofence) {
		super();
		this.topic = topic;
		this.geofence = geofence;
	}

	/*****************************************************************
	 * Getter & Setter
	 ****************************************************************/

	public Topic getTopic() {
		return topic;
	}

	public Geofence getGeofence() {
		return geofence;
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof UNSUBSCRIBEPayload)) {
			return false;
		}
		UNSUBSCRIBEPayload that = (UNSUBSCRIBEPayload) o;
		return Objects.equals(getTopic(), that.getTopic()) &&
				Objects.equals(getGeofence(), that.getGeofence());
	}

	@Override
	public int hashCode() {

		return Objects.hash(getTopic(), getGeofence());
	}
}
