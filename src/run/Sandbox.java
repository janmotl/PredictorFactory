package run;


import utility.Text;

import java.util.*;

public class Sandbox {

    public static void main(String[] arg) {
        //Set<String> set = new HashSet<>();
        SortedSet<String> set = new TreeSet<>();
        set.add("asd");
        set.add("ddd");

        System.out.println(set.toString());

        ////
        Text.string2list(null);
    }
}