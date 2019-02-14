package de.hasenburg.geobroker.model.payload;

import de.hasenburg.geobroker.model.Topic;
import de.hasenburg.geobroker.model.spatial.Geofence;

import java.util.Objects;

public class PUBLISHPayload extends AbstractPayload {

	protected Topic topic;
	protected Geofence geofence;
	protected String content;

	public PUBLISHPayload() {

	}

	public PUBLISHPayload(Topic topic, Geofence geofence, String content) {
		super();
		this.topic = topic;
		this.geofence = geofence;
		this.content = content;
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

	public String getContent() {
		return content;
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof PUBLISHPayload)) {
			return false;
		}
		PUBLISHPayload that = (PUBLISHPayload) o;
		return Objects.equals(getTopic(), that.getTopic()) &&
				Objects.equals(getGeofence(), that.getGeofence()) &&
				Objects.equals(getContent(), that.getContent());
	}

	@Override
	public int hashCode() {

		return Objects.hash(getTopic(), getGeofence(), getContent());
	}
}
