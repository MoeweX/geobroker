package de.hasenburg.geofencebroker.main.communication;

import de.hasenburg.geofencebroker.main.exceptions.DealerException;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Dealer Messager
 * 
 * @author jonathanhasenburg
 *
 */
public class Dealer {

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

	public Dealer(String address, int port) {
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
	 * Starts receiving and interpreting incoming envelopes..
	 * 
	 * @return a Future instance if reception was not running before and operation successful, <code>null</code> otherwise
	 */
	public Future<?> startReceiving() throws DealerException {
		if (runnableFuture != null && !runnableFuture.isDone()) {
			throw new DealerException("Dealer already running");
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
				throw new DealerException("Dealer could not be initialized");
			}
		} catch (InterruptedException | ExecutionException e) {
			throw new DealerException("Initialization was interrupted", e);
		}

		// init was successful
		runnableFuture = executor.submit(() -> {
			while (!Thread.currentThread().isInterrupted()) {
				byte[][] messageArray = new byte[6][];
				int index = 0;
				boolean messageFine = true;

				try {
					boolean more = true;
					while (more) {
						byte[] bytes = socket.recv(0);

						if (index >= 6) {
							System.out.println("Received more multipart messages than expected, "
									+ "dismissing: " + new String(bytes));
							messageFine = false;
						}

						messageArray[index] = bytes;
						more = socket.hasReceiveMore();
						index++;
					}
				} catch (ZMQException e) {
					System.out.println("Context was terminated, thread is dying");
					return;
				}

				if (!messageFine) {
					System.out.println("Message broken");
				} else {
					try {
						DealerMessage message = DealerMessage.build(messageArray);
						incrementNumberOfReceivedMessages();
						System.out.println("Received complete message from client " + message.getIdentity());
						interpretReceivedDealerMessage(message);
					} catch (DealerException e) {
						System.out.println(e.getMessage());
						e.printStackTrace();
					}
				}
			}
		});
		System.out.println("Started receiving incoming messages.");
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

	/**
	 * Gets automatically called after a complete message was received.
	 * 
	 * @param message - the received message
	 */
	public void interpretReceivedDealerMessage(DealerMessage message) {
		System.out.println(message.toString());

		Random random = new Random();

		if (random.nextBoolean()) {
			System.out.println("Sending bad id");
			socket.sendMore("badId");
			System.out.println("Sending bad delimiter");
			socket.sendMore("");
			System.out.println("Sending bad payload");
			socket.send("Broker Answer Impossible");
		} else {
			socket.sendMore(message.getIdentity());
			socket.sendMore("");
			socket.send("Broker Answer");
		}

	}

	public void sendMessageToClient(DealerMessage dealerMessage) {
		System.out.println("Sending message to " + dealerMessage.getIdentity());


	}

}
