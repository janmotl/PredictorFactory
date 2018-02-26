package meta;


import org.jetbrains.annotations.NotNull;
import utility.NullSafeComparator;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

// Important notice: The definition of fColumn and column is NOT "foreign key" and "primary key".
// It rather means "THAT" table contains fColumns anf "THIS" table contains column. And it does not matter which is
// primary and which is foreign. Mnemonic: "f" means "father".
// This difference is important during the propagation phase as we can propagate only from propagated tables. We
// do not care whether it is FK->PK or PK->FK. We just care that Propagated->Unpropagated works. But Unpropagated->Propagated
// does not work.

// Maybe we should name it Relationship rather than ForeignKey to avoid confusion.
@XmlType(name = "foreignConstraint")
public class ForeignConstraint implements Comparable<ForeignConstraint> {
	@XmlAttribute public String name;       // Optional in the input XML. More like a comment.
	public String fSchema;
	public String fTable;
	public String schema;
	public String table;
	public List<String> fColumn = new ArrayList<>();
	public List<String> column = new ArrayList<>();
	@XmlTransient public int sequence;      // Composite keys in the input XML use multiple column and fColumn fields.

	// Constructors
	public ForeignConstraint() {
	}

	public ForeignConstraint(String name, String fSchema, String fTable, String schema, String table, List<String> fColumn, List<String> column) {
		this.name = name;
		this.fSchema = fSchema;
		this.fTable = fTable;
		this.schema = schema;
		this.table = table;
		this.fColumn = fColumn;
		this.column = column;
	}

	// Required for assembling of composite keys (function collectRelationships)
	// Order by {table, fTable, name, sequence}. Name is nullable.
	@Override
	public int compareTo(@NotNull ForeignConstraint that) {
		final int BEFORE = -1;
		final int EQUAL = 0;
		final int AFTER = 1;

		// This optimization is usually worthwhile, and can always be added
		if (this == that) return EQUAL;

		// String comparisons
		int comparison = table.compareTo(that.table);
		if (comparison != EQUAL) return comparison;

		comparison = fTable.compareTo(that.fTable);
		if (comparison != EQUAL) return comparison;

		comparison = NullSafeComparator.nullSafeStringComparator(name, that.name);
		if (comparison != EQUAL) return comparison;

		// Primitive numbers follow this form
		if (sequence < that.sequence) return BEFORE;
		if (sequence > that.sequence) return AFTER;

		return EQUAL;
	}

	// Required for assembling of composite keys (function collectRelationships).
	// Compares based on {table, fTable, name} and intentionally ignores {sequence, column, fColumn}.
	// If name is null (name is nullable by JDBC), return false (as simple FKs are more common than compound FKs).
	// 	Name is frequently null in Teradata. Sadly, it means we do not currently support compound FKs in Teradata.
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ForeignConstraint that = (ForeignConstraint) o;

		if (table != null ? !table.equals(that.table) : that.table != null) return false;
		if (fTable != null ? !fTable.equals(that.fTable) : that.fTable != null) return false;
		return (name != null && name.equals(that.name));
	}

	@Override
	public int hashCode() {
		int result = table != null ? table.hashCode() : 0;
		result = 31 * result + (fTable != null ? fTable.hashCode() : 0);
		result = 31 * result + (name != null ? name.hashCode() : 0);
		return result;
	}

	// Note: this points from ALREADY PROPAGATED table to table TO PROPAGATE (not from FK to PK)!
	// See the explanation at the top of this class.
	@Override
	public String toString() {
		return "From this table to the father: " + schema + "." + table + " --> " + fSchema + "." + fTable;
	}
}
