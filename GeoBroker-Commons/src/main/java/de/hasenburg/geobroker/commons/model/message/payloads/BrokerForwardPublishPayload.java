package de.hasenburg.geobroker.commons.model.message.payloads;

import de.hasenburg.geobroker.commons.model.spatial.Location;

import java.util.Objects;

@SuppressWarnings("WeakerAccess") // as fields are checked by AbstractPayload.nullField()
public class BrokerForwardPublishPayload extends AbstractPayload {

	PUBLISHPayload publishPayload;
	Location publisherLocation;

	public BrokerForwardPublishPayload() {

	}

	public BrokerForwardPublishPayload(PUBLISHPayload publishPayload, Location publisherLocation) {
		super();
		this.publishPayload = publishPayload;
		this.publisherLocation = publisherLocation;
	}

	/*****************************************************************
	 * Getter & Setter
	 ****************************************************************/

	public PUBLISHPayload getPublishPayload() {
		return publishPayload;
	}

	public Location getPublisherLocation() {
		return publisherLocation;
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		BrokerForwardPublishPayload that = (BrokerForwardPublishPayload) o;
		return publishPayload.equals(that.publishPayload) && publisherLocation.equals(that.publisherLocation);
	}

	@Override
	public int hashCode() {
		return Objects.hash(publishPayload, publisherLocation);
	}
}
