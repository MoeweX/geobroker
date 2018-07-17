package de.hasenburg.geofencebroker.tester;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

public class Dealer {

	public static void main(String[] args) throws InterruptedException {
		Context context = ZMQ.context(1);

		//  Socket to talk to server
		Socket dealer = context.socket(ZMQ.DEALER);
		dealer.connect("tcp://localhost:5559");
		dealer.setIdentity("1".getBytes());

		ZMQ.Poller poller = context.poller();
		System.out.println("Launched and connected client.");

		while (!Thread.currentThread().isInterrupted()) {
			poller.register(dealer, ZMQ.Poller.POLLIN);
			long rc = poller.poll(0); // don't wait if empty
			System.out.println(rc + " object waiting to be polled");

			if (rc > 0) {
				// We got a reply from the server

				boolean more = true;
				byte[] message;
				while (more) {
					// receive message
					message = dealer.recv(0);
					more = dealer.hasReceiveMore();

					// print it
					System.out.println(new String(message));
				}

			}

			System.out.println("Sending two requests");
			// send 2 requests
			for (int request_nbr = 0; request_nbr < 2; request_nbr++) {
				dealer.sendMore("CONNECT");
				dealer.sendMore("CONNECT");
				dealer.sendMore("Topic");
				dealer.sendMore("GeoFence");
				dealer.send("Payload");
			}

			Thread.sleep(5000);

		}

		//  We never get here but clean up anyhow
		dealer.close();
		context.term();
	}

}
