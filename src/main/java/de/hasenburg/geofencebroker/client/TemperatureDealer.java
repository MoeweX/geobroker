package de.hasenburg.geofencebroker.client;

import de.hasenburg.geofencebroker.communication.RouterMessage;
import de.hasenburg.geofencebroker.exceptions.RouterException;
import org.zeromq.ZMQ;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TemperatureDealer {

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

	public TemperatureDealer(String address, int port, String identity) {

		context = ZMQ.context(1);

		//  Socket to talk to server
		socket = context.socket(ZMQ.DEALER);
		socket.connect(address + ":" + port);
		socket.setIdentity(identity.getBytes());
	}

	public Future<?> startReception() throws RouterException {
		if (runnableFuture != null && !runnableFuture.isDone()) {
			throw new RouterException("TemperatureDealer already running");
		}

		return null;
	}

	/**
	 * Stops the reception of messages immediately.
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

}
