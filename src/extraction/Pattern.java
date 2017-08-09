package extraction;

import connection.Parser;
import org.apache.log4j.Logger;
import run.Setting;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import java.io.File;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;


@XmlRootElement(name = "pattern")
public class Pattern {
	// Logging
	private static final Logger logger = Logger.getLogger(Pattern.class.getName());

	// Fields
	@XmlAttribute public int topN = 1; // The default value is to return the single best predictor per predictor.groupId.
	public String name;
	public String description;
	public String example;
	public String author;
	public String cardinality;
	public boolean requiresBaseDate;
	public LocalDate date;
	private List<PatternCode> code = new ArrayList<>();
	@XmlTransient public String dialectCode;    // For example std -> stddev_samp
	private List<PatternParameter> parameter = new ArrayList<>();
	@XmlTransient public SortedMap<String, String> dialectParameter = new TreeMap<>();
	private List<PatternOptimize> optimize = new ArrayList<>();
	@XmlTransient public List<OptimizeParameters> optimizeParameter = new ArrayList<>();

	public class OptimizeParameters {
		String key;
		double min;
		double max;
		boolean integerValue;
		int iterationLimit;
	}


	// SHOULD BE MOVED INTO A CONSTRUCTOR OF PREDICTOR (but note we need no-parameter constructor for JAXB)
	public void initialize(Setting setting) {
		agnostic2dialectCode(setting);
		setRequiresBaseDate();
	}


	// Load property list from XML
	public static Pattern unmarshall(String path) {
		Pattern pattern = new Pattern();

		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(Pattern.class);
			Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
			pattern = (Pattern) jaxbUnmarshaller.unmarshal(new File(path));
		} catch (JAXBException ignored) {
			logger.warn("Pattern " + path + " was not correctly unmarshalled.");
		}

		return pattern;
	}


	/////////// Subroutines ///////////
	// Convert the database agnostic SQL into vendor's specific SQL.
	// This conversion is performed just once to safe CPU power.
	// Do not enforce case sensitivity when we can't force the script designers to follow it (parameter "i").
	private void agnostic2dialectCode(Setting setting) {
		String rawCode = code.get(0).code;  // By default pick the first code

		// Pick the correct code
		for (PatternCode patternCode : code) {
			if (patternCode.compatibility != null && patternCode.compatibility.contains(setting.databaseVendor)) {
				rawCode = patternCode.code;
			}
		}

		// Translate the code
		dialectCode = Parser.getDialectCode(setting, rawCode);

		// Translate the parameters
		for (PatternParameter patternParameter : parameter) {
			dialectParameter.put(patternParameter.key, Parser.getDialectCode(setting, patternParameter.value));
		}

		// Convert constants in "optimize" into numbers
		for (PatternOptimize patternOptimize : optimize) {
			OptimizeParameters op = new OptimizeParameters();

			op.min = variable2value(setting, patternOptimize.min);
			op.max = variable2value(setting, patternOptimize.max);
			op.integerValue = patternOptimize.integerValue;
			op.iterationLimit = patternOptimize.iterationLimit;
			op.key = patternOptimize.key;

			optimizeParameter.add(op);
		}

	}

	// Subroutine - replace constants with the actual values.
	private static double variable2value(Setting setting, String input) {

		if ("@lagMax".equals(input)) return setting.lag;
		if ("@leadMin".equals(input)) return setting.lead;

		return Double.parseDouble(input);
	}


	private void setRequiresBaseDate() {
		requiresBaseDate = dialectCode.contains("@baseDate");
	}
}
