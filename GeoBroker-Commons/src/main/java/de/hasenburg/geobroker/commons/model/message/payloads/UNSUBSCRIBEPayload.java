package de.hasenburg.geobroker.commons.model.message.payloads;

import de.hasenburg.geobroker.commons.model.message.Topic;

import java.util.Objects;

public class UNSUBSCRIBEPayload extends AbstractPayload {

	protected Topic topic;

	public UNSUBSCRIBEPayload() {

	}

	public UNSUBSCRIBEPayload(Topic topic) {
		super();
		this.topic = topic;
	}

	/*****************************************************************
	 * Getter & Setter
	 ****************************************************************/

	public Topic getTopic() {
		return topic;
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		UNSUBSCRIBEPayload that = (UNSUBSCRIBEPayload) o;
		return Objects.equals(topic, that.topic);
	}

	@Override
	public int hashCode() {
		return Objects.hash(topic);
	}
}
