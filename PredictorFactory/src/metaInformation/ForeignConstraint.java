package metaInformation;


import java.util.ArrayList;
import java.util.List;

public class ForeignConstraint {
    public String name;
    public String table;
    public String fTable;
    public List<String> column = new ArrayList<>();
    public List<String> fColumn = new ArrayList<>();

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
                "name='" + name + "'" +
                ", table='" + table + "'" +
                ", fTable='" + fTable + "'" +
                ", column='" + column + "'" +
                ", fColumn='" + fColumn + "'" +
                "}";
    }
}
