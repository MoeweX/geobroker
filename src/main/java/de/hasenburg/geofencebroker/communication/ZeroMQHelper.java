package de.hasenburg.geofencebroker.communication;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

public class ZeroMQHelper {

	public static byte[][] receiveMessageArray(int numberOfFrames, ZMQ.Socket socket) {

		byte[][] messageArray = new byte[numberOfFrames][];
		int index = 0;
		boolean messageFine = true;


		boolean more = true;
		while (more) {
			byte[] bytes = socket.recv(0);

			if (index >= 6) {
				System.out.println("Received more message frames than expected, "
						+ "dismissing: " + new String(bytes));
				messageFine = false;
			}

			messageArray[index] = bytes;
			more = socket.hasReceiveMore();
			index++;
		}

		if (!messageFine) {
			System.out.println("Message broken");
			return null;
		} else {
			return messageArray;
		}

	}
}
