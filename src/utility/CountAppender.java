package utility;

import org.apache.log4j.Level;
import org.apache.log4j.WriterAppender;
import org.apache.log4j.spi.LoggingEvent;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;


// Count messages on different levers.
// This is important for detection of non-fatal errors that could otherwise go unnoticed.
// Source (originally for java.util.logger):
//  http://stackoverflow.com/questions/18470284/log-counter-in-java-logger
// Note: It is wasteful to store all messages. If all we want to get are the counts, keep in memory only the counts
// and just increment the right counter.
// Alternatives for Log4j: JAMonAppender
public class CountAppender extends WriterAppender {

	@NotNull private static List<LoggingEvent> records = new ArrayList<>();

	@Override
	public void append(LoggingEvent loggingEvent) {
		records.add(loggingEvent);
	}

	// Return the count of messages for the given level
	public static int getCount(@NotNull Level level) {
		int howMany = 0;
		for (LoggingEvent record : records) {
			if (record.getLevel().toInt() == level.toInt()) {
				howMany++;
			}
		}
		return howMany;
	}

}