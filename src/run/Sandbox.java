package run;


import utility.TextParser;

import java.util.SortedSet;
import java.util.TreeSet;

public class Sandbox {

	public static void main(String[] arg) {
		//Set<String> set = new HashSet<>();
		SortedSet<String> set = new TreeSet<>();
		set.add("asd");
		set.add("ddd");

		System.out.println(set.toString());

		////
		TextParser.string2list(null);
	}
}