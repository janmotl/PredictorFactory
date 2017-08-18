package utility;

import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FormatSQLinFormTest {

	@Test
	public void formatSQL_short() throws Exception {
		String sql = "SELECT base1, mode() WITHIN GROUP (ORDER BY column3) " +
				"   FROM propagatedTable1 " +
				"   GROUP BY base1";

		String expected = "SELECT \n" +
				"        base1, \n" +
				"        MODE() WITHIN GROUP (ORDER BY column3) \n" +
				"FROM \n" +
				"        propagatedTable1 \n" +
				"GROUP BY \n" +
				"        base1";

		Assert.assertEquals(expected, FormatSQLinForm.formatSQL(sql));
	}

	@Test
	public void formatSQL_over100Lines() throws Exception {
		String sql = "SELECT associations2.object_id, associations2.term_id, associations2.cat_ID, associations2.term_taxonomy_id, 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14 , 15 , 16 , 17 , 18 , 19 , 20 , 21 , 22 , 23 , 24 , 25 , 26 , 27 , 28 , 29 , 30 , 31 , 32 , 33 , 34 , 35 , 36 , 37 , 38 , 39 , 40 , 41 , 42 , 43 , 44 , 45 , 46 , 47 , 48 , 49 , 50 , 51 , 52 , 53 , 54 , 55 , 56 , 57 , 58 , 59 , 60 , 61 , 62 , 63 , 64 , 65 , 66 , 67 , 68 , 69 , 70 , 71 , 72 , 73 , 74 , 75 , 76 , 77 , 78 , 79 , 80 , 81 , 82 , 83 , 84 , 85 , 86 , 87 , 88 , 89 , 90 , 91 , 92 , 93 , 94 , 95 , 96 , 97 , 98 , 99 , 100\n" +
				"FROM (SELECT objects_tags.object_id, objects_tags.term_id, wp_cb_tags2cats.cat_ID, categories.term_taxonomy_id\n" +
				"    FROM (SELECT wp_term_relationships.object_id, wp_term_taxonomy.term_id, wp_term_taxonomy.term_taxonomy_id\n" +
				"        FROM wp_term_relationships\n" +
				"        LEFT JOIN wp_term_taxonomy ON wp_term_relationships.term_taxonomy_id = wp_term_taxonomy.term_taxonomy_id\n" +
				"        ORDER BY object_id ASC, term_id ASC) \n" +
				"        AS objects_tags\n" +
				"    LEFT JOIN wp_cb_tags2cats ON objects_tags.term_id = wp_cb_tags2cats.tag_ID\n" +
				"    LEFT JOIN (SELECT wp_term_relationships.object_id, wp_term_taxonomy.term_id as cat_ID, wp_term_taxonomy.term_taxonomy_id\n" +
				"        FROM wp_term_relationships\n" +
				"        LEFT JOIN wp_term_taxonomy ON wp_term_relationships.term_taxonomy_id = wp_term_taxonomy.term_taxonomy_id\n" +
				"        WHERE wp_term_taxonomy.taxonomy = 'category'\n" +
				"        GROUP BY object_id, cat_ID, term_taxonomy_id\n" +
				"        ORDER BY object_id, cat_ID, term_taxonomy_id) \n" +
				"        AS categories on wp_cb_tags2cats.cat_ID = categories.term_id\n" +
				"    WHERE objects_tags.term_id = wp_cb_tags2cats.tag_ID\n" +
				"    GROUP BY object_id, term_id, cat_ID, term_taxonomy_id\n" +
				"    ORDER BY object_id ASC, term_id ASC, cat_ID ASC) \n" +
				"    AS associations2\n" +
				"LEFT JOIN categories ON associations2.object_id = categories.object_id\n" +
				"WHERE associations2.cat_ID <> categories.cat_ID\n" +
				"GROUP BY object_id, term_id, cat_ID, term_taxonomy_id\n" +
				"ORDER BY object_id, term_id, cat_ID, term_taxonomy_id";

		// Validation
		String formattedSQL = FormatSQLinForm.formatSQL(sql);
		Matcher m = Pattern.compile("\r\n|\r|\n").matcher(formattedSQL);
		int lines = 1;
		while (m.find())
		{
		    lines ++;
		}

		Assert.assertTrue(lines>101);
	}


	@Test
	public void formatColourful() throws Exception {
		String sql = "SELECT base1, mode() WITHIN GROUP (ORDER BY column3) " +
				"   FROM propagatedTable1 " +
				"   GROUP BY base1";

		Assert.assertTrue(FormatSQLinForm.formatColourful(sql).startsWith("<html>"));
	}


}