package de.hasenburg.geobroker.commons.model.message.payloads;

import java.util.Objects;

@SuppressWarnings("WeakerAccess") // as fields are checked by AbstractPayload.nullField()
public class BrokerForwardDisconnectPayload extends AbstractPayload {

	String clientIdentifier;
	DISCONNECTPayload disconnectPayload;

	public BrokerForwardDisconnectPayload(String clientIdentifier, DISCONNECTPayload disconnectPayload) {
		super();
		this.clientIdentifier = clientIdentifier;
		this.disconnectPayload = disconnectPayload;
	}

	/*****************************************************************
	 * Getter & Setter
	 ****************************************************************/

	public String getClientIdentifier() {
		return clientIdentifier;
	}

	public DISCONNECTPayload getDisconnectPayload() {
		return disconnectPayload;
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		BrokerForwardDisconnectPayload that = (BrokerForwardDisconnectPayload) o;
		return Objects.equals(clientIdentifier, that.clientIdentifier) && Objects.equals(disconnectPayload,
				that.disconnectPayload);
	}

	@Override
	public int hashCode() {
		return Objects.hash(clientIdentifier, disconnectPayload);
	}
}
