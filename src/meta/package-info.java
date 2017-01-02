
// JAXB: Isn't it possible to use an XmlAdapter without @XmlJavaTypeAdapter?
//  http://stackoverflow.com/questions/6857166/jaxb-isnt-it-possible-to-use-an-xmladapter-without-xmljavatypeadapter
//  http://stackoverflow.com/questions/6110757/jaxb-xml-adapters-work-via-annotations-but-not-via-setadapter

// package-info.class content in jaxb context
//  http://stackoverflow.com/questions/10205977/package-info-class-content-in-jaxb-context

// Alternatives
//  http://www.eclipse.org/eclipselink/documentation/2.4/moxy/type_level002.htm

@XmlJavaTypeAdapters({
		@XmlJavaTypeAdapter(value = LocalDateAdapter.class, type = LocalDate.class),
		@XmlJavaTypeAdapter(value = LocalDateTimeAdapter.class, type = LocalDateTime.class)
})
package meta;

import utility.LocalDateAdapter;
import utility.LocalDateTimeAdapter;

import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapters;
import java.time.LocalDate;
import java.time.LocalDateTime;