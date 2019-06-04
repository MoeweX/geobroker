package de.hasenburg.geobroker.commons;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class BenchmarkHelper {

	private static final Logger logger = LogManager.getLogger();
	private static final ExecutorService pool = Executors.newSingleThreadExecutor();

	public static final int CAPACITY = 100000;
	public static final int FLUSH_COUNT = CAPACITY / 2;
	public static final String directoryPath = "./benchmarking_results/";
	public static AtomicBoolean benchmarking = new AtomicBoolean(false);

	private static final BlockingQueue<BenchmarkEntry> benchmarkEntryStorage = new ArrayBlockingQueue<>(CAPACITY);
	private static final AtomicInteger putElementCount = new AtomicInteger(0);

	public static void startBenchmarking() {
		File f = new File(directoryPath);
		if (f.mkdirs() || f.exists()) {
			BenchmarkHelper.benchmarking.set(true);
			logger.info("Activated benchmarking");
		} else {
			logger.error("Could not activate benchmarking");
		}
	}

	public static void addEntry(String name, long time) {
		if (!benchmarking.get()) {
			return;
		}

		BenchmarkEntry entry = new BenchmarkEntry(name, System.currentTimeMillis(), time);
		int timeToFlush = putElementCount.incrementAndGet() % FLUSH_COUNT;
		benchmarkEntryStorage.offer(entry);
		if (timeToFlush == 0) {
			// FLUSH_COUNT elements have been added since last flush, so time to flush
			pool.submit(() -> {
				logger.info("Flushing benchmarking values");
				try {
					BufferedWriter writer = new BufferedWriter(new FileWriter(directoryPath + System.nanoTime() + ".csv"));
					writer.write(BenchmarkEntry.getCSVHeader());
					for (int i = 0; i < FLUSH_COUNT; i++) {
						writer.write(String.valueOf(benchmarkEntryStorage.poll()));
					}
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
		}
	}

	public static void stopBenchmarking() {
		BenchmarkHelper.benchmarking.set(false);
		pool.submit(() -> {
			logger.info("Flushing benchmarking values");
			try {
				BufferedWriter writer = new BufferedWriter(new FileWriter(directoryPath + System.nanoTime() + ".csv"));
				writer.write(BenchmarkEntry.getCSVHeader());
				for (int i = 0; i < benchmarkEntryStorage.size(); i++) {
					writer.write(String.valueOf(benchmarkEntryStorage.poll()));
				}
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		pool.shutdown();
		try {
			pool.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			logger.error("Did not terminate properly", e);
		}
		logger.info("Deactivated benchmarking");
	}

	public static class BenchmarkEntry {
		public String name;
		public long timestamp;
		public long time;

		public BenchmarkEntry(String name, long timestamp, long time) {
			this.name = name;
			this.timestamp = timestamp;
			this.time = time;
		}

		public static BenchmarkEntry fromString(String s) {
			String[] entries = s.split(";");
			if (entries.length == 3) {
				return new BenchmarkEntry(entries[0], Long.valueOf(entries[1]), Long.valueOf(entries[2]));
			} else {
				return new BenchmarkEntry("null", 0, 0);
			}
		}

		public static String getCSVHeader() {
			return "name;timestamp;time\n";
		}

		@Override
		public String toString() {
			return String.format("%s;%d;%d\n", name, timestamp, time);
		}
	}

	/*****************************************************************
	 * Post-Processing
	 ****************************************************************/

	// contains the post-processing file names for raw data
	private ArrayList<String> filePaths = new ArrayList<>();

	public BenchmarkHelper() {

	}

	public void sortIntoFiles() throws IOException {
		logger.info("Starting sort process");
		File directory = new File(BenchmarkHelper.directoryPath);
		HashMap<String, BufferedWriter> writers = new HashMap<>();

		// read in each file
		for (File f : Objects.requireNonNull(directory.listFiles())) {
			logger.info("Starting with file {}", f.getName());
			Stream<String> stream = Files.lines(Paths.get(f.getPath()));
			stream.forEach(line -> {
				try {
					// we do not want the header
					if (!line.startsWith(BenchmarkEntry.getCSVHeader().substring(0, 5))) {

						BenchmarkEntry entry = BenchmarkEntry.fromString(line);

						// get correct writer or create
						BufferedWriter writer = writers.get(entry.name);
						if (writer == null) {
							logger.info("Creating writer for {}", entry.name);
							String filePath = BenchmarkHelper.directoryPath + entry.name + ".csv";
							filePaths.add(filePath);
							writer = new BufferedWriter(new FileWriter(new File(filePath)));
							writers.put(entry.name, writer);
							writer.write(BenchmarkEntry.getCSVHeader());
						}

						// write to correct writer
						writer.write(entry.toString());
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
			logger.info("Finished file {}", f.getName());
		}
		// close all writers
		for (BufferedWriter writer : writers.values()) {
			writer.close();
		}
	}

	public void writeStatisticsForFile(String filePath) throws IOException {
		DescriptiveStatistics stats = new DescriptiveStatistics();

		// read in data
		Stream<String> stream = Files.lines(Paths.get(filePath));
		stream.forEach(line -> {
			// we do not want the header
			if (!line.startsWith(BenchmarkEntry.getCSVHeader().substring(0, 5))) {

				BenchmarkEntry entry = BenchmarkEntry.fromString(line);
				// no null entries should exist, let's check anyhow
				if (entry.name == null) {
					throw new RuntimeException("Nulls not supported");
				}
				stats.addValue(entry.time);

			}
		});

		double n = stats.getN();
		double mean = stats.getMean();
		double std = stats.getStandardDeviation();
		double median = stats.getPercentile(50);

		BufferedWriter writer = new BufferedWriter(new FileWriter(filePath.replaceFirst(".csv", "") + "-stats.csv"));
		writer.write("n;" + n + "\n");
		writer.write("mean;" + mean + "\n");
		writer.write("standard deviation;" + std + "\n");
		writer.write("median;" + median + "\n");
		writer.close();
	}

	public static void main (String[] args) throws IOException {
	    BenchmarkHelper helper = new BenchmarkHelper();
	    helper.sortIntoFiles();
		for (String path : helper.filePaths) {
			helper.writeStatisticsForFile(path);
		}
	}

}
