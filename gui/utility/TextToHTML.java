package utility;

import com.github.rjeschke.txtmark.Processor;

public class TextToHTML {

    // Converts text to HTML.
    // Puts <p> to the front.
    // Empty lines are replaced with: </p><p>
    // Puts </p> to the end.
    // Replaces tabs with spaces.
    // Replaces multiple consequent spaces with a single space.
    // Removes heading and tailing whitespaces.
    // If null is passed, an empty String is returned.
    // We are lazy and use Markdown processor to do it.
    public static String textToHTML(String text) {

        if (text == null) return "";

        text = text.replace("\t", " ");                 // Remove tabs to not format text as REM
        text = text.replaceAll("(?si)note.*", "");      // Ignore everything behind NOTE: keyword
        text = "[$PROFILE$]: extended \n" + text;       // Tell the markdown processor to ignore underscores
        text = Processor.process(text);                 // Markdown to HTML

        return text;
    }
}
