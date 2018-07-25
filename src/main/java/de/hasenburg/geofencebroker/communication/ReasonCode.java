package de.hasenburg.geofencebroker.communication;

@SuppressWarnings("SpellCheckingInspection")
public enum ReasonCode {

	NormalDisconnection,
	ProtocolError,
	NotConnected,
	GrantedQoS1,
	Success,

	// New Reason Codes
	LocationUpdated
}
