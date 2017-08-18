package utility;

import javafx.scene.paint.Color;
import sqlinformfx.SQLForm;
import sqlinformfx.Utility;

import javax.swing.text.DefaultStyledDocument;
import javax.swing.text.Document;
import javax.swing.text.StyledDocument;
import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class FormatSQLinForm {

	// Returns plain text
	public static String formatSQL(String sql) {
		SQLForm sqlForm = new SQLForm();

		sqlForm.a(true);                                // Ignore100LineLimit
		sqlForm.m(sql);                                 // Pass the unformatted SQL
		String formattedSQLasString = sqlForm.a();      // Format

		return formattedSQLasString;
	}

	// Returns coloured output
	// It is ~5 time faster than TGSqlParser and it correctly parses all SQLs I tried.
	// It just need better styling.
	public static String formatColourful(String sql) {
		StyledDocument styledDocument = new DefaultStyledDocument();    // Creation of this class takes 300ms, the rest is fast
		SQLForm sqlForm = new SQLForm();

		// Set colors,... For additional parameters see FormatSQLLinFormOptions.setGUIOptions()
        sqlForm.h(Utility.a(Color.BLACK));      // masterKeyword
        sqlForm.i(Utility.a(Color.BLACK));      // lineComment
        sqlForm.j(Utility.a(Color.BLACK));      // blockComment
        sqlForm.k(Utility.a(Color.BLACK));      // CASEKeyword
        sqlForm.l(Utility.a(Color.BLACK));      // literal
        sqlForm.m(Utility.a(Color.BLACK));      // reservedWord
        sqlForm.n(Utility.a(Color.BLACK));      // FROM, GROUP BY,...

		sqlForm.a(true);                        // Ignore100LineLimit
		sqlForm.m(sql);                         // formatStatement()
		sqlForm.a((Document) styledDocument);   // generateColouredOutput()

		return sqlForm.a(styledDocument);       // writeHtmlBody()
	}

	// Convenience method for debugging
	public static void displayHtmlInBrowser(String content) {
		File file = new File("test.html");

		try {
			Files.write(file.toPath(), content.getBytes());
			Desktop.getDesktop().browse(file.toURI());
		} catch (IOException e) {
			// TODO Auto-generated catch block
		}
	}

	}
