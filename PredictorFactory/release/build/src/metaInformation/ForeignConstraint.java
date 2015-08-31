package metaInformation;


import java.util.*;

public class ForeignConstraint implements Comparable<ForeignConstraint> {
    public String table;
    public String fTable;
    public List<String> column = new ArrayList<>();
    public List<String> fColumn = new ArrayList<>();

    // Take a list of foreign key constraints as provided by JDBC driver and convert them
    // to a list of ForeignConstrain objects. This is done because ForeignConstrain object
    // supports both, simple and composite constraints.
    // The single table name is shared with all the constrains in the input list.
    public static List<ForeignConstraint> jdbcToList(List<List<String>> jdbcList, String table) {

        // Initialization
        List<ForeignConstraint> result = new ArrayList<ForeignConstraint>();

        // Loop
        for (List<String> constraint : jdbcList) {
            ForeignConstraint fc = new ForeignConstraint(table, constraint.get(0), constraint.get(1), constraint.get(2));
            result.add(fc);
        }

        return result;
    }


    // Combine composite constrains together.
    // BEWARE OF NULL STRINGS.
    // THE ALGORITHM DOESN'T DIFFERENTIATE BETWEEN MULTIPLE SINGLE CONSTRAINS AND A SINGLE COMPOSITE CONSTRAINT!
    public static List<ForeignConstraint> combine(List<ForeignConstraint> list){

        // Initialization
        List<ForeignConstraint> result = new ArrayList<>(); // The result list

        // Sort by {table, fTable}
        list.sort(Comparator.<ForeignConstraint>naturalOrder());

        // Pad the list at the beginning and the end
        list.add(0, new ForeignConstraint("", "", "", ""));
        list.add(new ForeignConstraint("", "", "", ""));

        // Make one pass over the list of constrains
        for (int i = 1; i < list.size()-1; i++) {
            ForeignConstraint lagFc = list.get(i - 1);
            ForeignConstraint fc = list.get(i);
            ForeignConstraint leadFc = list.get(i + 1);

            String lagName = lagFc.table + "." + lagFc.fTable;
            String name = fc.table + "." + fc.fTable;
            String leadName = leadFc.table + "." + leadFc.fTable;

            if (!lagName.equals(name) && !name.equals(leadName)) { // A simple constrain
                result.add(fc);
            } else if (!lagName.equals(name) && name.equals(leadName)) { // Beginning of a composite constrain
                ForeignConstraint composite = new ForeignConstraint(fc.table, fc.fTable, fc.column.get(0), fc.fColumn.get(0));
                result.add(composite);
            } else { // Continue building the composite constrain
                result.get(result.size()-1).column.add(fc.column.get(0));
                result.get(result.size()-1).fColumn.add(fc.fColumn.get(0));
            }
        }


        return result;
    }

    // For the use in SQL.propagateID
    public Map<String, String> getMap() {

        // Initialization
        Map<String, String> result = new TreeMap<>();
        int idNumber;

        // Loop
        idNumber = 1; // We are indexing from 1 by 2
        for (String columnName : column) {
            result.put("@idColumn" + idNumber, columnName);
            idNumber = idNumber + 2;
        }

        idNumber = 2; // We are indexing from 2 by 2
        for (String columnName : fColumn) {
            result.put("@idColumn" + idNumber, columnName);
            idNumber = idNumber + 2;
        }

        return result;
    }

    ////////////////// Boring stuff //////////////
    // Constructor
    public ForeignConstraint(String table, String fTable, String column, String fColumn) {
        this.table = table;
        this.fTable = fTable;
        this.column.add(column);
        this.fColumn.add(fColumn);
    }

    public ForeignConstraint(String table, String fTable, List<String> column, List<String> fColumn) {
        this.table = table;
        this.fTable = fTable;
        this.column = column;
        this.fColumn = fColumn;
    }

    // Sort by {table, fTable}
    @Override
    public int compareTo(ForeignConstraint o) {
        int lastCmp = table.compareTo(o.table);
        return (lastCmp != 0 ? lastCmp : fTable.compareTo(o.fTable));
    }

    @Override
    public String toString() {
        return "FC{" +
                "table='" + table + '\'' +
                ", fTable='" + fTable + '\'' +
                ", column='" + column + '\'' +
                ", fColumn='" + fColumn + '\'' +
                '}';
    }
}
