

@XmlJavaTypeAdapters({
		@XmlJavaTypeAdapter(value = LocalDateAdapter.class, type = LocalDate.class),
		@XmlJavaTypeAdapter(value = LocalDateTimeAdapter.class, type = LocalDateTime.class)
})
@XmlAccessorType(XmlAccessType.FIELD)
package extraction;

import utility.LocalDateAdapter;
import utility.LocalDateTimeAdapter;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapters;
import java.time.LocalDate;
import java.time.LocalDateTime;