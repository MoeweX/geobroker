package de.hasenburg.geofencebroker.communication;

import de.hasenburg.geofencebroker.exceptions.RouterException;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import java.util.concurrent.*;

/**
 * Router Messager
 * 
 * @author jonathanhasenburg
 *
 */
public class Router {

	/**
	 * The address used for the reception of messages.
	 */
	private final String address;

	/**
	 * The port used for the reception of messages.
	 */
	private final int port;

	/**
	 * The number of messages that have been received until now
	 */
	private int numberOfReceivedMessages = 0;

	/**
	 * The executor that is used to execute the runnable which is used for reception.
	 */
	private final ExecutorService executor = Executors.newSingleThreadExecutor();

	/**
	 * Future of the runnable which is used for reception.
	 */
	private Future<?> runnableFuture = null;

	private ZMQ.Context context = null;
	private ZMQ.Socket socket = null;

	public Router(String address, int port) {
		this.address = address;
		this.port = port;
	}

	/**
	 * @return {@link #address}
	 */
	public String getAddress() {
		return address;
	}

	/**
	 * @return {@link #port}
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @return {@link #numberOfReceivedMessages}
	 */
	public int getNumberOfReceivedMessages() {
		return numberOfReceivedMessages;
	}

	/**
	 * Increments {@link #numberOfReceivedMessages} by one.
	 */
	protected void incrementNumberOfReceivedMessages() {
		numberOfReceivedMessages++;
	}


	/**
	 * Starts message reception.
	 *
	 * @param messageQueue - the queue to which incoming messages are added, can be null if #interpretReceivedRouterMessage should be run
	 *
	 * @return a Future instance if reception was not running before and operation successful, <code>null</code> otherwise
	 */
	public Future<?> startReceptionAndAddMessagesToQueue(BlockingQueue<RouterMessage> messageQueue) throws RouterException {
		if (runnableFuture != null && !runnableFuture.isDone()) {
			throw new RouterException("Router already running");
		}

		Future<Boolean> fut = executor.submit(() -> {
			if (executor.isShutdown()) {
				return false;
			}

			context = ZMQ.context(1);
			socket = context.socket(ZMQ.ROUTER);
			// socket.setRouterMandatory(true);

			socket.bind(getAddress() + ":" + getPort());
			return true;
		});

		try {
			if (!fut.get()) {
				throw new RouterException("Router could not be initialized");
			}
		} catch (InterruptedException | ExecutionException e) {
			throw new RouterException("Initialization was interrupted", e);
		}

		// init was successful
		runnableFuture = executor.submit(() -> {

			while (!Thread.currentThread().isInterrupted()) {
				byte[][] messageArray = ZeroMQHelper.receiveMessageArray(6, socket);

				if (messageArray != null) {
					try {
						// A complete message was received -> store in queue
						RouterMessage message = RouterMessage.build(messageArray);
						incrementNumberOfReceivedMessages();
						System.out.println("Received complete message from client " + message.getIdentity());

						if (!messageQueue.offer(message)) {
							System.out.println("Could not add message to queue, dropping it.");
						}
					} catch (RouterException e) {
						System.out.println(e.getMessage());
						e.printStackTrace();
					}
				}
			}
		});
		System.out.println("Started message reception, storing messages in given queue");
		return runnableFuture;
	}

	/**
	 * Stops the reception of envelopes immediately.
	 */
	public void stopReception() {
		executor.shutdown();
		if (runnableFuture != null) {
			runnableFuture = null;
			socket.close();
			context.term();
		}
		executor.shutdownNow();
		System.out.println("Reception of incoming messages is stopped."
				+ (executor.isTerminated() ? "" : " Some tasks may still be running."));
	}

	/**
	 * Checks whether reception of envelopes is currently running.
	 * 
	 * @return <code>true</code> if currently receiving, false otherwise
	 */
	public boolean isReceiving() {
		return runnableFuture != null && !runnableFuture.isDone();
	}

	public void sendMessageToClient(RouterMessage routerMessage) {
		System.out.println("Sending message to " + routerMessage.getIdentity());
	}

}
