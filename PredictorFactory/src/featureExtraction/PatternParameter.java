package featureExtraction;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

@XmlType(name="parameter")
public class PatternParameter {
	@XmlAttribute String key;
	@XmlAttribute String value;
}
