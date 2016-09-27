package utility;

import javafx.application.Platform;
import javafx.scene.control.TextArea;
import org.apache.log4j.WriterAppender;
import org.apache.log4j.spi.LoggingEvent;

import java.util.LinkedList;
import java.util.List;

/**
 * Print logs into a TextArea.
 * For details see: http://www.rshingleton.com/javafx-log4j-textarea-log-appender/
 *
 */
public class TextAreaAppender extends WriterAppender {

    private static TextArea textArea = null;            // The text area
    private static final int messageCountLimit = 100;   // Maximum lines allowed in the text area. Must be bigger than 1.
    private static String text = "...The log window is limited to the last " + messageCountLimit + " messages. The whole log is in the 'log' directory.\n";
    private static List<Integer> messageLengthList = new LinkedList<>(); // Lengths of messages within the text area

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
    
        // Extract the message
        final String message = this.layout.format(loggingEvent);

        // Append formatted message to the text area using the Thread (we don't want to block the rest of GUI).
        // Protect the text area from flooding with text by limiting the amount of messages to display.
        try {
            Platform.runLater(new Runnable() {
                @Override
                public void run() {
                                    
                    if (messageLengthList.size() > messageCountLimit) {
                        textArea.deleteText(messageLengthList.get(0), messageLengthList.get(0) + messageLengthList.get(1));
                        messageLengthList.remove(1);
                    } else if (messageLengthList.size() == messageCountLimit) {
                        textArea.replaceText(0, messageLengthList.get(0), text);
                        messageLengthList.remove(0);
                        messageLengthList.add(0, text.length());
                    }
                
                    textArea.appendText(message);
                    messageLengthList.add(message.length());
        
                }
            });
        } catch (IllegalStateException e) {
            System.out.println("Unable to append the log to the text area.");
        }

    }



}