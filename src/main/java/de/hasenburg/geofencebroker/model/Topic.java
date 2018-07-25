package de.hasenburg.geofencebroker.model;

import java.util.Objects;

public class Topic {

	private String topic;

	private Topic() {
		// JSON
	}

	public Topic(String topic) {
		this.topic = topic;
	}

	public String getTopic() {
		return topic;
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof Topic)) {
			return false;
		}
		Topic topic1 = (Topic) o;
		return Objects.equals(getTopic(), topic1.getTopic());
	}

	@Override
	public int hashCode() {

		return Objects.hash(getTopic());
	}

	@Override
	public String toString() {
		return getTopic();
	}
}
