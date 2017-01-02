package utility;


import org.commonmark.html.HtmlRenderer;
import org.commonmark.node.Node;
import org.commonmark.parser.Parser;

public class TextToHTMLAtlassian {


	public static void main(String[] args) {
		Parser parser = Parser.builder().build();
		Node document = parser.parse("This is *Sparta* \n~~~sql\n select * from table\n~~~");

		HtmlRenderer renderer = HtmlRenderer.builder().build();
		System.out.println(renderer.render(document));  // "<p>This is <em>Sparta</em></p>\n"

	}


}
