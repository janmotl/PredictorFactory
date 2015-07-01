package utility;

import java.io.File;
import java.util.SortedMap;
import java.util.TreeMap;

import featureExtraction.Pattern;

public class PatternMap {

	// Return list of all patterns in the directory. 
	// Note that returned patterns do not have vendor specific code - you still have to call agnostic2dialectCode.
	// This is because the conversion requires filled Setting object, which may not be always available.  
	public static SortedMap<String, Pattern> getPatternMap() {

		// Initialize the output
		SortedMap<String, Pattern> outputMap = new TreeMap<String, Pattern>();
		
		// Get list of file paths
		File dir = new File("pattern");
		File[] directoryListing = dir.listFiles(); 	// May return null
		
		// Parse each file and convert them into a predictor
		if (directoryListing != null) {
			for (File path : directoryListing) {			
				Pattern pattern = Pattern.unmarshall(path.toString());		// Read a pattern
				if (pattern != null) {										// If pattern was correctly read...
					outputMap.put(pattern.name, pattern);					// ... add it into the map of patterns.
				}
			}
		}
		
		return outputMap;
	}
}
