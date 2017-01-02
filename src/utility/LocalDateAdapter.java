package utility;

import org.jetbrains.annotations.NotNull;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.time.LocalDate;

// XML doesn't know how to deal with LocalDate, hence we teach it.
// This class is used in Pattern.java via binding.
public class LocalDateAdapter extends XmlAdapter<String, LocalDate> {

	public LocalDate unmarshal(@NotNull String v) {
		return LocalDate.parse(v);
	}

	@NotNull public String marshal(@NotNull LocalDate v) {
		return v.toString();
	}

}