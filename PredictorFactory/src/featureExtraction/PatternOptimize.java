package featureExtraction;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

@XmlType(name="optimize")
public class PatternOptimize {
	@XmlAttribute String key;
	@XmlAttribute String min;
	@XmlAttribute String max;
	@XmlAttribute Boolean integerValue;
	@XmlAttribute Integer iterationLimit;
}
