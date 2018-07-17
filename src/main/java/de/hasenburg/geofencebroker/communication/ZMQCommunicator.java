package de.hasenburg.geofencebroker.communication;

import de.hasenburg.geofencebroker.exceptions.CommunicatorException;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public abstract class ZMQCommunicator {

	/*****************************************************************
	 * Fields
	 ****************************************************************/

	// Address and port to connect or bind to
	protected String address;
	protected int port;

	// Thread management for receiving messages
	private final ExecutorService executor = Executors.newSingleThreadExecutor();
	private Future<?> runnableFuture = null;

	// Socket and context
	protected ZMQ.Context context = null;
	protected ZMQ.Socket socket = null;

	// Others
	private int numberOfReceivedMessages = 0;

	/*****************************************************************
	 * Constructors and (De-)Initializer
	 ****************************************************************/

	public ZMQCommunicator(String address, int port) {
		this.address = address;
		this.port = port;
	}

	/**
	 * @param identity - if null, a random identity is chosen
	 */
	public abstract void init(String identity);

	public void tearDown() {
		stopReceiving();
		socket.close();
		context.term();
	}

	/*****************************************************************
	 * Receiving Messages
	 ****************************************************************/

	public void startReceiving(BlockingQueue<ZMsg> messageQueue) throws CommunicatorException {

		if (isReceiving()) {
			throw new CommunicatorException("ZMQCommunicator already receives messages.");
		}

		runnableFuture = executor.submit(() -> {
			while (!Thread.currentThread().isInterrupted()) {

				ZMsg message = ZMsg.recvMsg(socket);
				System.out.println("Received message " + message.toString());

				if (!messageQueue.offer(message)) {
					System.out.println("Could not add message to queue, dropping it.");
				}
				numberOfReceivedMessages++;
			}
		});

		System.out.println("Started receiving messages, messages are stored in given queue");
	}

	public void stopReceiving() {
		runnableFuture.cancel(true);
		runnableFuture = null;
		System.out.println("Reception of incoming messages is stopped."
				+ (executor.isTerminated() ? "" : " Some tasks may still be running."));
	}

	public boolean isReceiving() {
		return runnableFuture != null && !runnableFuture.isDone();
	}

	/*****************************************************************
	 * Sending Messages
	 ****************************************************************/

	public void sendMessage(ZMsg message) {
		if (socket.getType() == ZMQ.DEALER) {
			System.out.println("Sending message to Server");
		} else if (socket.getType() == ZMQ.ROUTER) {
			System.out.println("Sending message to " + message.getFirst().toString());
		}

		message.send(socket);
	}

}
