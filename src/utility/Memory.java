package utility;

import org.apache.log4j.Logger;

import java.text.DecimalFormat;
import java.text.NumberFormat;


public class Memory {

	// Logging
	private static final Logger logger = Logger.getLogger(Memory.class.getName());

	/**
	 * Returns used memory in MB
	 */
	public static double usedMemory() {
		Runtime runtime = Runtime.getRuntime();
		return usedMemory(runtime);
	}

	/**
	 * Returns reserved memory in MB
	 */
	public static double reservedMemory() {
		Runtime runtime = Runtime.getRuntime();
		return reservedMemory(runtime);
	}

	/**
	 * Returns max available memory in MB
	 */
	public static double maxMemory() {
		Runtime runtime = Runtime.getRuntime();
		return maxMemory(runtime);
	}

	/**
	 * Log memory consumption
	 */
	public static void logMemoryInfo() {

		// Initialization
		Runtime runtime = Runtime.getRuntime();
		double usedMemory = usedMemory(runtime);
		NumberFormat f = new DecimalFormat("###,##0.0");

		// Logging
		logger.debug("Used memory: " + f.format(usedMemory) + " MB");
	}


	// Subroutines
	private static double usedMemory(Runtime runtime) {
		long totalMemory = runtime.totalMemory();
		long freeMemory = runtime.freeMemory();
		double usedMemory = (double) (totalMemory - freeMemory) / (double) (1024 * 1024);
		return usedMemory;
	}

	private static double maxMemory(Runtime runtime) {
		long maxMemory = runtime.maxMemory();
		double memory = (double) maxMemory / (double) (1024 * 1024);
		return memory;
	}

	private static double reservedMemory(Runtime runtime) {
		long reservedMemory = runtime.totalMemory();
		double memory = (double) reservedMemory / (double) (1024 * 1024);
		return memory;
	}


}

