package metaInformation;


import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;


@XmlType(name="foreignConstraint")
public class ForeignConstraint implements Comparable<ForeignConstraint> {
    @XmlAttribute public String name;       // Optional in the input XML. More like a comment.
    public String table;
    public String fTable;
    public List<String> column = new ArrayList<>();
    public List<String> fColumn = new ArrayList<>();
    @XmlTransient public int sequence;      // Composite keys in the input XML use multiple column and fColumn fields.

    ////////////////// Boring stuff //////////////

    // Constructor
    public ForeignConstraint() {
    }

    public ForeignConstraint(String name, String table, String fTable, List<String> column, List<String> fColumn) {
        this.name = name;
        this.table = table;
        this.fTable = fTable;
        this.column = column;
        this.fColumn = fColumn;
    }

    // Required for assembling of composite keys (function collectRelationships)
    @Override
    public int compareTo(ForeignConstraint that) {
        final int BEFORE = -1;
        final int EQUAL = 0;
        final int AFTER = 1;

        // This optimization is usually worthwhile, and can always be added
        if (this == that) return EQUAL;

        int comparison = this.table.compareTo(that.table);
        if (comparison != EQUAL) return comparison;

        comparison = this.fTable.compareTo(that.fTable);
        if (comparison != EQUAL) return comparison;
           
        comparison = this.name.compareTo(that.name);
        if (comparison != EQUAL) return comparison;

        // Primitive numbers follow this form
        if (this.sequence < that.sequence) return BEFORE;
        if (this.sequence > that.sequence) return AFTER;
       
        return EQUAL;
    }
   
    // Required for assembling of composite keys (function collectRelationships)
    // Compares based on {table, fTable, name} and intentionally ignores {sequence, column, fColumn}
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ForeignConstraint that = (ForeignConstraint) o;

        if (table != null ? !table.equals(that.table) : that.table != null) return false;
        if (fTable != null ? !fTable.equals(that.fTable) : that.fTable != null) return false;
        return !(name != null ? !name.equals(that.name) : that.name != null);
    }

    @Override
    public int hashCode() {
        int result = table != null ? table.hashCode() : 0;
        result = 31 * result + (fTable != null ? fTable.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "FC{" +
                "name='" + name + "'\n" +
                ", table='" + table + "'\n" +
                ", fTable='" + fTable + "'\n" +
                ", column='" + column + "'\n" +
                ", fColumn='" + fColumn + "'\n" +
                ", seq='" + sequence + "'" +
                "}";
    }

}
