package de.hasenburg.geofencebroker.tester;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

public class Request {

	public static void main(String[] args) {
		Context context = ZMQ.context(1);

		//  Socket to talk to server
		Socket requester = context.socket(ZMQ.REQ);
		requester.connect("tcp://localhost:5559");
		requester.setIdentity("1".getBytes());

		System.out.println("Launched and connected client.");

		for (int request_nbr = 0; request_nbr < 10; request_nbr++) {
			requester.sendMore("CONNECT");
			requester.sendMore("Topic");
			requester.sendMore("GeoFence");
			requester.send("Payload");
			String reply = requester.recvStr(0);
			System.out.println("Received reply " + request_nbr + " [" + reply + "]");
		}

		//  We never get here but clean up anyhow
		requester.close();
		context.term();
	}

}
