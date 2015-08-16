package metaInformation;


import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ForeignConstraintTest {


    @Test
    public void combine() {
        // Initialization
        ForeignConstraint simple1 = new ForeignConstraint("t1", "t2", "id_s1", "id");
        ForeignConstraint simple2 = new ForeignConstraint("t2", "t3", "id_s2", "id");
        ForeignConstraint simple3 = new ForeignConstraint("t2", "t4", "id_s3", "id");
        ForeignConstraint simple4 = new ForeignConstraint("t3", "t2", "id_s4", "id");
        ForeignConstraint comp1a = new ForeignConstraint("t2", "t5", "id1", "id1");
        ForeignConstraint comp1b = new ForeignConstraint("t2", "t5", "id2", "id2");
        ForeignConstraint comp1c = new ForeignConstraint("t2", "t5", "id3", "id3");
        ForeignConstraint comp2a = new ForeignConstraint("t2", "t6", "id1", "id1");
        ForeignConstraint comp2b = new ForeignConstraint("t2", "t6", "id2", "id2");

        List<ForeignConstraint> fcList = new ArrayList<>();
        fcList.add(simple1);
        fcList.add(simple4);
        fcList.add(simple3);
        fcList.add(comp2b);
        fcList.add(comp2a);
        fcList.add(comp1a);
        fcList.add(comp1b);
        fcList.add(comp1c);
        fcList.add(simple2);

        // Execution
        List<ForeignConstraint> list = ForeignConstraint.combine(fcList);

        // Validation
        Assert.assertEquals(6, list.size());
        Assert.assertEquals(1, list.get(2).column.size());
        Assert.assertEquals(3, list.get(3).column.size());
        Assert.assertEquals(2, list.get(4).column.size());
    }

    @Test
    public void jdbcToList() {
        // Initialization
        List<String> a = new ArrayList<>();
        a.add("t1");
        a.add("id1");
        a.add("id2");
        List<String> b = new ArrayList<>();
        b.add("t2");
        b.add("id3");
        b.add("id4");

        List<List<String>> jdbcList = new ArrayList<>();
        jdbcList.add(a);
        jdbcList.add(b);

        // Execution
        List<ForeignConstraint> list = ForeignConstraint.jdbcToList(jdbcList, "t0");
        Map<String, String> mapA = list.get(0).getMap();
        Map<String, String> mapB = list.get(1).getMap();

        // Validation
        Assert.assertEquals(2, list.size());
        Assert.assertEquals(2, mapA.size());
        Assert.assertEquals(2, mapB.size());
    }
}