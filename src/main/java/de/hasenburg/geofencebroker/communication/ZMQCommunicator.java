package de.hasenburg.geofencebroker.communication;

import de.hasenburg.geofencebroker.main.Utility;
import de.hasenburg.geofencebroker.model.exceptions.CommunicatorException;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public abstract class ZMQCommunicator {

	private final Logger logger;

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
	 * Constructors and Co
	 ****************************************************************/

	public ZMQCommunicator(String address, int port, Logger logger) {
		this.address = address;
		this.port = port;
		this.logger = logger;
	}

	/**
	 * @param identity - if null, a random identity is chosen
	 */
	public abstract void init(String identity);

	public void tearDown() {
		executor.shutdown();
		if (this.runnableFuture != null) {
			this.runnableFuture = null;
			socket.setLinger(0);
			socket.close();
			context.term();
		}

		this.executor.shutdownNow();
		logger.info("Tear down completed, communicator cannot be reused." + (executor.isTerminated() ? "" : " Some tasks may still be running."));
	}

	public String getIdentity() {
		return new String(socket.getIdentity());
	}

	/*****************************************************************
	 * Receiving Messages
	 ****************************************************************/

	public void startReceiving(BlockingQueue<ZMsg> messageQueue) throws CommunicatorException {

		if (executor.isShutdown()) {
			throw new CommunicatorException("ZMQCommunicator shutdown, cannot be reused.");
		}

		if (isReceiving()) {
			throw new CommunicatorException("ZMQCommunicator already receives messages.");
		}

		runnableFuture = executor.submit(() -> {
			while (!Thread.currentThread().isInterrupted()) {

				ZMsg message = ZMsg.recvMsg(socket);
				if (SocketType.DEALER.equals(socket.getSocketType())) {
					logger.trace("Received InternalClientMessage: {}", message.toString());
				} else if (SocketType.ROUTER.equals(socket.getType())) {
					logger.trace("Received InternalBrokerMessage: {}", message.toString());
				}

				if (!messageQueue.offer(message)) {
					logger.warn("Could not add message to queue, dropping it.");
				}
				numberOfReceivedMessages++;
			}
		});

		logger.debug("Started receiving messages, messages are stored in given queue");
	}

	public boolean isReceiving() {
		return runnableFuture != null && !runnableFuture.isDone();
	}

	/*****************************************************************
	 * Sending Messages
	 ****************************************************************/

	protected synchronized void sendMessage(ZMsg message) {
		if (SocketType.DEALER.equals(socket.getSocketType())) {
			logger.trace("Sending InternalClientMessage: {}", message.toString());
		} else if (SocketType.ROUTER.equals(socket.getSocketType())) {
			logger.trace("Sending InternalBrokerMessage: {}", message.toString());
		}

		message.send(socket);
	}

}
