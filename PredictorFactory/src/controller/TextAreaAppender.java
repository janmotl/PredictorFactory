package controller;

import javafx.application.Platform;
import javafx.scene.control.TextArea;

import org.apache.log4j.WriterAppender;
import org.apache.log4j.spi.LoggingEvent;
 
/**
 * Print logs into a TextArea.
 * For details see: http://www.rshingleton.com/javafx-log4j-textarea-log-appender/
 * 
 */
public class TextAreaAppender extends  WriterAppender {
 
    private static TextArea textArea = null;
 
    /**
     * Set the target TextArea for the logging information to appear.
     *
     * @param textArea
     */
    public void setTextArea(TextArea textArea) {
        TextAreaAppender.textArea = textArea;
    }
 
        
    /**
     * Format and then append the loggingEvent to the stored TextArea.
     *
     * @param loggingEvent
     */
	@Override
	public void append(LoggingEvent loggingEvent) {
		
		final String message = this.layout.format(loggingEvent);

		// Append formatted message to the text area using the Thread.
		try {
			Platform.runLater(new Runnable() {
				@Override
				public void run() {
					textArea.appendText(message);
				}
			});
		} catch (IllegalStateException e) {
			System.out.println("Unable to append the log to the text area.");
		}

	}
    
}