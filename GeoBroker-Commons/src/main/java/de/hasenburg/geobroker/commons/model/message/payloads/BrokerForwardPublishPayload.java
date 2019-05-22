package de.hasenburg.geobroker.commons.model.message.payloads;

import de.hasenburg.geobroker.commons.model.spatial.Location;

import java.util.Objects;

@SuppressWarnings("WeakerAccess") // as fields are checked by AbstractPayload.nullField()
public class BrokerForwardPublishPayload extends AbstractPayload {

	PUBLISHPayload publishPayload;
	Location publisherLocation; // needed in case of matching at the subscriber
	String subscriberClientIdentifier; // needed in case of matching at the publisher

	public BrokerForwardPublishPayload() {

	}

	public BrokerForwardPublishPayload(PUBLISHPayload publishPayload, Location publisherLocation) {
		super();
		this.publishPayload = publishPayload;
		this.publisherLocation = publisherLocation;
		this.subscriberClientIdentifier = "empty";
	}

	public BrokerForwardPublishPayload(PUBLISHPayload publishPayload, String subscriberClientIdentifier) {
		super();
		this.publishPayload = publishPayload;
		this.publisherLocation = Location.undefined();
		this.subscriberClientIdentifier = subscriberClientIdentifier;
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

	public String getSubscriberClientIdentifier() {
		return subscriberClientIdentifier;
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		BrokerForwardPublishPayload that = (BrokerForwardPublishPayload) o;
		return Objects.equals(publishPayload, that.publishPayload) && Objects.equals(publisherLocation,
				that.publisherLocation) && Objects.equals(subscriberClientIdentifier, that.subscriberClientIdentifier);
	}

	@Override
	public int hashCode() {
		return Objects.hash(publishPayload, publisherLocation, subscriberClientIdentifier);
	}
}
