package extraction;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlValue;

@XmlType(name = "code")
public class PatternCode {
	@XmlAttribute String dialect;
	@XmlValue String code;
}
