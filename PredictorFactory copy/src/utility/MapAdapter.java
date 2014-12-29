package utility;

import java.util.HashMap;
import java.util.Map;


import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.adapters.XmlAdapter;


class MapElements {
    @XmlAttribute
    public String key;
    @XmlAttribute
    public String value;

    @SuppressWarnings("unused")    
	private MapElements() {
    } //Required by JAXB

    
    public MapElements(String key, String value) {
        this.key = key;
        this.value = value;
    }
}

// Should have been used 
public class MapAdapter extends XmlAdapter<MapElements[], Map<String, String>> {


	// Marshal (java -> XML)
	public MapElements[] marshal(Map<String, String> arg0) throws Exception {
		MapElements[] mapElements = new MapElements[arg0.size()];
		int i = 0;
		for (Map.Entry<String, String> entry : arg0.entrySet())
			mapElements[i++] = new MapElements(entry.getKey(), entry.getValue());

		return mapElements;
	}

	// Unmarshal (XML -> java)
	public Map<String, String> unmarshal(MapElements[] arg0) throws Exception {
		Map<String, String> r = new HashMap<String, String>();
		for (MapElements mapelement : arg0)
			r.put(mapelement.key, mapelement.value);
		return r;
	}
}