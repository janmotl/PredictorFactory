package run;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

//Defines root element of the XML file
@XmlRootElement
//Defines order in which elements are created in the XML file
@XmlType(propOrder = { "name", "description", "author", "date", "code" ,"parameter", "cardinality"})

public final class Pattern {
	// Settings
	private static final String[] KEYWORDLIST = { "column", "inputTable", "time" };

	// Parameters
	private String name;
	private String description;
	private String author;
	private LocalDate date;
	private String code;
	private String cardinality;
	private Map<String, String> parameter = new HashMap<String, String>();

    public static class MapEntry {  
        @XmlAttribute  
        public String key;  
        @XmlAttribute  
        public String value;  
    }  
	

	// Constructor
	public Pattern() {		

	};

	// Get meta information about the pattern
	// Should be named getTableList and getColumnList
	public int[] getParameterCount() {
		// Initialization
		int[] keywordOccurence = new int[KEYWORDLIST.length];

		// Count occurrence of each keyword in the code
		for (int i = 0; i < keywordOccurence.length; i++) {
			String regexPattern = "(.)(@" + KEYWORDLIST[i] + ")(.)";
			keywordOccurence[i] = 0;
			Matcher m = java.util.regex.Pattern.compile(regexPattern).matcher(
					code);
			while (m.find()) {
				keywordOccurence[i]++;
			}
		}

		// Return
		return keywordOccurence;
	}

	// Get all parameters of the pattern
	// NOT USED
	public ArrayList<String> getParameters() {
		// Initialization
		ArrayList<String> parameterList = new ArrayList<String>();

		// Count occurrence of each keyword in the code
		for (String keyword : KEYWORDLIST) {
			String regexPattern = "@" + keyword + "\\d*";
			Matcher m = java.util.regex.Pattern.compile(regexPattern).matcher(
					code);
			while (m.find()) {
				parameterList.add(m.group());
			}
		}

		// Return
		return parameterList;
	}
	
	// Return list of columns
	public ArrayList<String> getColumnList() {
		// Initialization
		ArrayList<String> columnList = new ArrayList<String>();

		// Return each occurrence of "@*column*" in the code. The search is case insensitive.
		String regexPattern = "@.*COLUMN.*";
		Matcher m = java.util.regex.Pattern.compile(regexPattern).matcher(code.toUpperCase());
		while (m.find()) {
			columnList.add(m.group());
		}
		
		// Return
		return columnList;
	}

	// Get parameterList
//	public String[] popParameterList() {
//		String[] parameterList = parameter.get("@aggregateFunction").split(",");
//	}
	
	
	

	//////////// Setters and getters //////////
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public LocalDate getDate() {
		return date;
	}

	@XmlJavaTypeAdapter(utility.LocalDateAdapter.class)
	@XmlElement(name = "date")
	public void setDate(LocalDate date) {
		this.date = date;
	}
	
	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getCardinality() {
		return cardinality;
	}

	public void setCardinality(String cardinality) {
		this.cardinality = cardinality;
	}

	// ArrayList version because marshaller doesn't work with HashMap natively. Used by JAXB exclusively.
	// Should have been done in adapter together with MapEntry object
    @XmlElement(name = "parameter")  
    private MapEntry[] getParameter() {  
        List<MapEntry> list = new ArrayList<MapEntry>();  
        for (Entry<String, String> entry : parameter.entrySet()) {  
            MapEntry mapEntry =new MapEntry();  
            mapEntry.key = entry.getKey();  
            mapEntry.value = entry.getValue();  
            list.add(mapEntry);  
        }  
        return list.toArray(new MapEntry[list.size()]);  
    }  
      
    @SuppressWarnings("unused")
	private void setParameter(MapEntry[] arr) {  
        for(MapEntry entry : arr) {  
            parameter.put(entry.key, entry.value);  
        }  
    }  
	
    // HashMap version for use in Predictor Factory (but not for JAXB)
    @XmlTransient
	public Map<String, String> getParameterMap() {
		return parameter;
	}

	public void setParameterMap(Map<String, String> parameter) {
		this.parameter = parameter;
	}


	

}
