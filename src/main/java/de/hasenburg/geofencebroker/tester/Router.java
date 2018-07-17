package de.hasenburg.geofencebroker.tester;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

public class Router {

	public static void main(String[] args) {
		// prepare our context and sockets
		Context context = ZMQ.context(1);

		Socket socket = context.socket(ZMQ.ROUTER);
		socket.bind("tcp://*:5559");

		System.out.println("Launched and bind router broker.");


		boolean more = true;
		byte[] message;

		while (!Thread.currentThread().isInterrupted()) {

			System.out.println("Waiting for message");

			while (more) {
				// receive message
				message = socket.recv(0);
				more = socket.hasReceiveMore();

				// print it
				System.out.println(new String(message));
			}

			more = true;

		}
		//  we never get here but clean up anyhow
		socket.close();
		context.term();
	}

}
