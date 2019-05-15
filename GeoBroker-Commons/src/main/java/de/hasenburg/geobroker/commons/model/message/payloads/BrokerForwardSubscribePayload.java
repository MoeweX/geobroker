package de.hasenburg.geobroker.commons.model.message.payloads;

import java.util.Objects;

@SuppressWarnings("WeakerAccess") // as fields are checked by AbstractPayload.nullField()
public class BrokerForwardSubscribePayload extends AbstractPayload {

	SUBSCRIBEPayload subscribePayload;

	public BrokerForwardSubscribePayload() {

	}

	public BrokerForwardSubscribePayload(SUBSCRIBEPayload subscribePayload) {
		super();
		this.subscribePayload = subscribePayload;
	}

	/*****************************************************************
	 * Getter & Setter
	 ****************************************************************/

	public SUBSCRIBEPayload getSubscribePayload() {
		return subscribePayload;
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		BrokerForwardSubscribePayload that = (BrokerForwardSubscribePayload) o;
		return Objects.equals(subscribePayload, that.subscribePayload);
	}

	@Override
	public int hashCode() {
		return Objects.hash(subscribePayload);
	}
}
