package de.hasenburg.geofencebroker.communication;

import de.hasenburg.geofencebroker.model.DealerMessage;
import de.hasenburg.geofencebroker.model.Location;
import de.hasenburg.geofencebroker.model.PayloadPINGREQ;
import de.hasenburg.geofencebroker.model.RouterMessage;
import de.hasenburg.geofencebroker.model.exceptions.CommunicatorException;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class DealerCommunicator extends ZMQCommunicator {

	public DealerCommunicator(String address, int port) {
		super(address, port);
	}

	@Override
	public void init(String identity) {
		context = ZMQ.context(1);
		socket = context.socket(ZMQ.DEALER);

		if (identity != null) { socket.setIdentity(identity.getBytes()); }

		socket.connect(address + ":" + port);
	}

	public void sendDealerMessage(DealerMessage message) {
		sendMessage(message.getZmsg());
	}

	public static void main(String[] args) throws CommunicatorException, InterruptedException {
		DealerCommunicator dealer = new DealerCommunicator("tcp://localhost", 5559);
		Random random = new Random();
		dealer.init("Dealer-" + random.nextInt(9999));
		BlockingQueue<ZMsg> blockingQueue = new LinkedBlockingDeque<>();
		dealer.startReceiving(blockingQueue);

		while (!Thread.currentThread().isInterrupted()) {
			dealer.sendMessage(ZMsg.newStringMsg("CONNECT", "geofence", "topic", "payload"));
			Thread.sleep(1000);
		}
	}

}
