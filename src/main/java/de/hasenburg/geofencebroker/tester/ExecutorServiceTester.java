package de.hasenburg.geofencebroker.tester;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecutorServiceTester {

	public static void main(String[] args) {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.submit(() -> {
			System.out.println("Starting sleep 1");
			ExecutorServiceTester.interruptSleep(2000);
			System.out.println("Stopping sleep 1");
		});
		executor.submit(() -> {
			System.out.println("Starting sleep 2");
			ExecutorServiceTester.interruptSleep(2000);
			System.out.println("Stopping sleep 2");
		});

	}

	public static void interruptSleep(int millis) {
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
}
