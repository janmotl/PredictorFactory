package featureExtraction;

import org.apache.log4j.Logger;
import parser.ANTLR;
import run.Setting;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.*;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.io.File;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;


@XmlRootElement (name="pattern")
@XmlAccessorType(XmlAccessType.FIELD)
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
	@XmlJavaTypeAdapter(LocalDateAdapter.class) public LocalDate date;
	private ArrayList<PatternCode> code = new ArrayList<>();
	@XmlTransient public String dialectCode;	// For example std -> stddev_samp
	private ArrayList<PatternParameter> parameter = new ArrayList<>();
	@XmlTransient public SortedMap<String, String> dialectParameter = new TreeMap<>();
	private ArrayList<PatternOptimize> optimize = new ArrayList<>();
	@XmlTransient public ArrayList<OptimizeParameters> optimizeParameter  = new ArrayList<>();
	
	public class OptimizeParameters {
		String key;
		double min;
		double max;
		boolean integerValue;
		int iterationLimit;
	}
	
	// Constructor
	public Pattern(){}

	// SHOULD BE MOVED INTO A CONSTRUCTOR OF PREDICTOR
	public void initialize(Setting setting) {
		agnostic2dialectCode(setting);
		setRequiresBaseDate();
	}


	  
	// Load property list from XML
	public static Pattern unmarshall(String path){
		Pattern list = null;
	    
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(Pattern.class);
		    Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		    list = (Pattern) jaxbUnmarshaller.unmarshal( new File(path) ); 
		} catch (JAXBException e) {
			logger.warn("Pattern " + path + " was not correctly unmarshalled.");
		}

	    return list;
	}
	
	
	/////////// Subroutines ///////////
	// Convert the database agnostic SQL into vendor's specific SQL.
	// This conversion is performed just once to safe CPU power.
	// Do not enforce case sensitivity when we can't force the script designers to follow it (parameter "i").
	private void agnostic2dialectCode(Setting setting) {
		String rawCode = code.get(0).code;	// By default pick the first code

		// Pick the correct code
		for (PatternCode patternCode : code) {
			if (patternCode.compatibility!=null && patternCode.compatibility.contains(setting.databaseVendor)) {
				rawCode = patternCode.code;
			}
		}

		// Translate the code
		dialectCode = getDialectString(setting, rawCode);

		// Translate the parameters
		for (PatternParameter patternParameter : parameter) {
			dialectParameter.put(patternParameter.key, getDialectString(setting, patternParameter.value));
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

	// Subroutine - get vendor's dialect. 
	// For example std -> stddev_samp.
	// Protected because of testing
	protected static String getDialectString(Setting setting, String agnosticCode) {
		// Stddev_samp
		String dialectCode = agnosticCode.replaceAll("(?i)stdDev_samp", setting.stdDevCommand);
		
		// charLengthCommand
		dialectCode = dialectCode.replaceAll("(?i)char_length", setting.charLengthCommand);
		
		// Timestamp
		dialectCode = dialectCode.replaceAll("(?i)timestamp", setting.typeTimestamp);
		
		// DateDiff, dateToNumber...
		dialectCode = ANTLR.parseSQL(setting, dialectCode);
		
		// NullIf (SAS doesn't support nullIf command in SQL over JDBC, rewrite nullIf with basic commands)
		if ("SAS".equals(setting.databaseVendor)) {
			dialectCode = nullIf(dialectCode);
		}
		
		
		return dialectCode;
	}

	// Subroutine for getDialectString
	private static String nullIf(String dialectCode) {
		java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("(?is)(.*)(nullif\\()(.*?)(,+)(.*?)(\\))(.*)");
		
		do {
			Matcher matcher = pattern.matcher(dialectCode);
			if (matcher.find())
			{		    
			    // Extract the keywords
			    String what = matcher.group(3);
			    String value = matcher.group(5);
					
				// Replace
				String caseWhen = "CASE WHEN @what = @value THEN null ELSE @what END";
				caseWhen = caseWhen.replace("@what", what);
				caseWhen = caseWhen.replace("@value", value);
				dialectCode = matcher.group(1) + caseWhen + matcher.group(7);
			} else {
				break;
			}
		} while (true);
		
		return dialectCode;
	}

	private void setRequiresBaseDate() {
		requiresBaseDate = dialectCode.contains("@baseDate");
	}
}
