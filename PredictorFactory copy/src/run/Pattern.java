package run;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
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

	// Parameters
	private String name;
	private String description;
	private String author;
	private LocalDate date;
	private String code;
	private String cardinality;
	private SortedMap<String, String> parameter = new TreeMap<String, String>();

    public static class MapEntry {  
        @XmlAttribute  
        public String key;  
        @XmlAttribute  
        public String value;  
    }  
	

	// Constructor
	public Pattern() {

	};

	// Return set of columns to bind. The set is obtained by parsing the code.
	public SortedSet<String> getColumnSet() {
		// Initialization
		SortedSet<String> columnSet = new TreeSet<String>();

		// Return each occurrence of "@?*column*" in the code. The search is (for simplicity) case sensitive.
		String regexPattern = "@\\w+Column\\w*";
		Matcher m = java.util.regex.Pattern.compile(regexPattern).matcher(code);
		while (m.find()) {
			columnSet.add(m.group());
		}
		
		// Return
		return columnSet;
	}


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
	
    // Map version for use in Predictor Factory (but not for JAXB)
    @XmlTransient
	public SortedMap<String, String> getParameterMap() {
		return parameter;
	}

	public void setParameterMap(SortedMap<String, String> map) {
		this.parameter = map;
	}


	

}
