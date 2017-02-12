package utility;

import sqlinformfx.ParsedSQLTree;
import sqlinformfx.SQLForm;
import sqlinformfx.SQLMinimalHTMLWriter;

import javax.swing.text.DefaultStyledDocument;
import javax.swing.text.Document;
import javax.swing.text.StyledDocument;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class FormatSQLinForm {

	public static void main(String[] args) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException {
		String sql = "SELECT base1, mode() WITHIN GROUP (ORDER BY column3) " +
				"   FROM propagatedTable1 " +
				"   GROUP BY base1";

		// Initialization
		SQLForm sqlForm = new SQLForm();
		Document document = new DefaultStyledDocument();

		String formattedSQLasHTML = "";

		// Plain text
		sqlForm.m(sql);                                 // Pass the unformatted SQL
		String formattedSQLasString = sqlForm.a();      // Format
		System.out.println(formattedSQLasString);       // Print it out

		// Tree
		ParsedSQLTree SQLTree = sqlForm.b();




		// HTML
		try {
			sqlForm.a(document);                                        // Invoke: void a(Document)
			formattedSQLasHTML = sqlForm.a((StyledDocument)document);   // Invoke: String a(DefaultStyledDocument)
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		System.out.println(formattedSQLasHTML);


		// Color text
		sqlForm.J(true);
		sqlForm.L(false);

		Method method = sqlForm.getClass().getDeclaredMethod("K");
		method.setAccessible(true);
		method.invoke(sqlForm);

		Field field = sqlForm.getClass().getDeclaredField("K");
		field.setAccessible(true);
		Object colorText = field.get(sqlForm);

		System.out.println(colorText);
	}


	public static String a(StyledDocument p_doc) {
		String l_HTMLText = "";
		StringWriter l_writer = new StringWriter();
		SQLMinimalHTMLWriter l_htmlWriter = new SQLMinimalHTMLWriter(l_writer, p_doc);

		try {
			l_htmlWriter.write();
			l_HTMLText = l_writer.toString();
			l_HTMLText = l_HTMLText.replaceAll(">[\\s]+", ">");
			l_HTMLText = l_HTMLText.replaceAll("[\\s]+<", "<");
			l_HTMLText = l_HTMLText.replaceAll("</p>", "</br>");
			l_HTMLText = l_HTMLText.replaceAll("<p class=default>", "");
			l_HTMLText = l_HTMLText.replaceAll("color: #b4b9be", "background: #b4b9be");
			l_HTMLText = l_HTMLText.replaceAll("<body>", "<body> <font face=\"Courier New\"><span style=\"white-space: nowrap; font-size:100%\">");
		} catch (Exception var7) {
			l_HTMLText = "Error 201 in SQLDocument";
		}

		return l_HTMLText;
	}
}
