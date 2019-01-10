package de.hasenburg.geofencebroker.communication;

@SuppressWarnings("SpellCheckingInspection")
public enum ReasonCode {

	NormalDisconnection,
	ProtocolError,
	NotConnected,
	GrantedQoS0,
	Success,
	NoMatchingSubscribers,

	// New Reason Codes
	LocationUpdated
}
