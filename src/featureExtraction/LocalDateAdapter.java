package featureExtraction;

import java.time.LocalDate;

import javax.xml.bind.annotation.adapters.XmlAdapter;

// XML doesn't know how to deal with LocalTime, hence we teach it. 
// This class is used in Pattern.java via binding.
public class LocalDateAdapter extends XmlAdapter<String, LocalDate> {

	public LocalDate unmarshal(String v){
		return LocalDate.parse(v);
	}

	public String marshal(LocalDate v){
		return v.toString();
	}

}