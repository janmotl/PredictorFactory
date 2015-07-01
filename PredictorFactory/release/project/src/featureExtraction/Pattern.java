package featureExtraction;

import java.io.File;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.log4j.Logger;

import run.Setting;


@XmlRootElement (name="pattern")
@XmlAccessorType(XmlAccessType.FIELD)
public class Pattern {
	// Logging
	public static final Logger logger = Logger.getLogger(Pattern.class.getName());
		
	// Fields
	@XmlAttribute public int topN = 1; // The default value is to return a single best predictor per predictor.groupId.
	public String name;
	public String description;
	public String author;
	public String cardinality;
	@XmlJavaTypeAdapter(LocalDateAdapter.class) public LocalDate date;
	private ArrayList<PatternCode> code = new ArrayList<PatternCode>();
	@XmlTransient public String dialectCode;	// For example std -> stddev_samp
	private ArrayList<PatternParameter> parameter = new ArrayList<PatternParameter>();
	@XmlTransient public SortedMap<String, String> dialectParameter = new TreeMap<String, String>();
	private ArrayList<PatternOptimize> optimize = new ArrayList<PatternOptimize>();
	@XmlTransient public ArrayList<OptimizeParameters> optimizeParameter  = new ArrayList<OptimizeParameters>();
	
	public class OptimizeParameters {
		String key;
		double min;
		double max;
		boolean integerValue;
		int iterationLimit;
	}
	
	// Constructor
	public Pattern(){}
	
    // Convert the database agnostic SQL into vendor's specific SQL.
	// This conversion is performed just once to safe CPU power.
	// Do not enforce case sensitivity when we can't force the script designers to follow it (parameter "i").
	// SHOULD BE DONE IN PREDICTOR, NOT IN PATTERN!
	public void agnostic2dialectCode(Setting setting) {		
		String rawCode = code.get(0).code;	// By default pick the first code 
		
		// Pick the correct code
		for (PatternCode patternCode : code) {
			if (patternCode.compatibility!=null && patternCode.compatibility.contains(setting.databaseVendor)) {
				rawCode = patternCode.code;
			}
		}
		
		// Translate the code
		dialectCode = getDialect(setting, rawCode);
		
		// Translate the parameters
		for (PatternParameter patternParameter : parameter) {
			dialectParameter.put(patternParameter.key, getDialect(setting, patternParameter.value));
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
	
	// Subroutine - replace constants with the actual values.
	private double variable2value(Setting setting, String input) {
		
		if ("@lagMax".equals(input)) return setting.lag;
		if ("@leadMin".equals(input)) return setting.lead;
		
		return Double.parseDouble(input);
	}
	

	// Subroutine - get vendor's dialect. 
	// For example std -> stddev_samp.
	// Protected because of testing
	protected String getDialect(Setting setting, String agnosticCode) {
		// Stddev_samp
		String dialectCode = agnosticCode.replaceAll("(?i)stdDev_samp", setting.stdDevCommand);
		
		// charLengthCommand
		dialectCode = dialectCode.replaceAll("(?i)char_length", setting.charLengthCommand);
		
		// Timestamp
		dialectCode = dialectCode.replaceAll("(?i)timestamp", setting.typeTimestamp);
		
		// DateDiff. Ignore line breaks with "s" parameter.
		java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("(?is)(.*)dateDiff\\((.*),(.*)\\)(.*)");	
		
		do {
			Matcher matcher = pattern.matcher(dialectCode);
			if (matcher.find())
			{		    
			    // Extract the keywords
			    String dateTo = matcher.group(2);
			    String dateFrom = matcher.group(3);
					
				// Replace
				String dateDiff = setting.dateDiffSyntax.replace("@dateFrom", dateFrom);
				dateDiff = dateDiff.replace("@dateTo", dateTo);
				dialectCode = matcher.group(1) + dateDiff + matcher.group(4);
			} else {
				break;
			}
		} while (true);
		

		
		return dialectCode;
	}
}
