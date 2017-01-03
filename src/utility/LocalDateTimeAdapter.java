package utility;

import org.jetbrains.annotations.NotNull;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.time.LocalDateTime;

// XML doesn't know how to deal with LocalDateTime, hence we teach it.
// This class is used in Predictor.java via binding.
public class LocalDateTimeAdapter extends XmlAdapter<String, LocalDateTime> {

	public LocalDateTime unmarshal(String v) {
		return LocalDateTime.parse(v);
	}

	public String marshal(LocalDateTime v) {
		return v.toString();
	}

}

