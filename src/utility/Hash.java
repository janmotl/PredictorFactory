package utility;

import meta.OutputTable;

// Calculates a hash from a String.
// This hash was developed for a concise and deterministic identification of the generated tables in the database.
// Originally we were using a counter, which we incremented for each table. However, if some tables got omitted
// during the two-stage processing, the identifiers shifted. And the queries failed. To remedy this, we use this hash.
// The properties of the hash:
//  1) It is short (3 chars)
//  2) It is using 37 safe characters (26 capital letters from English alphabet, 10 digits and underscore)
// Reference:
//  https://stackoverflow.com/questions/2624192/good-hash-function-for-strings
public class Hash {

    private static final int PRIME = 50653; // Because the biggest prime ≤ 37^3 is 50653; we use a prime to reduce the probability of colisions as much as possibly
    private static final char[] ALPHABET = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z','_'};
    private static final String SEPARATOR = " ('`;||*=) "; // We don't want to use a separator that can be a part of a table name. Example: table1+separator+table2 should not equal table3.

    // We want to hash the whole propagation path including the last hop.
    // We use complete specification including the schema name because we support multiple input schemas
    // and target table in the output schema.
    // Note: We do not need to hash the whole path - it should be enough to concatenate(father_hash, the_last_hop).
    // Note: We do not need to concatenate the Strings first. See:
    //  https://stackoverflow.com/questions/11537005/why-chose-31-to-do-the-multiplication-in-the-hashcode-implementation
    public static String deterministically(OutputTable table) {
        String concatenated = "";

        // All previous hops
        for (OutputTable propagationTable : table.propagationTables) {
            if (propagationTable.propagationForeignConstraint != null) {
                concatenated =  concatenated +
                                propagationTable.propagationForeignConstraint.schema + SEPARATOR +
                                propagationTable.propagationForeignConstraint.table + SEPARATOR +
                                propagationTable.propagationForeignConstraint.column + SEPARATOR +
                                propagationTable.propagationForeignConstraint.fSchema + SEPARATOR +
                                propagationTable.propagationForeignConstraint.fTable + SEPARATOR +
                                propagationTable.propagationForeignConstraint.fColumn + SEPARATOR;
            }
        }

        // The last hop
        concatenated =  concatenated +
                        table.propagationForeignConstraint.schema + SEPARATOR +
                        table.propagationForeignConstraint.table + SEPARATOR +
                        table.propagationForeignConstraint.column + SEPARATOR +
                        table.propagationForeignConstraint.fSchema + SEPARATOR +
                        table.propagationForeignConstraint.fTable + SEPARATOR +
                        table.propagationForeignConstraint.fColumn + SEPARATOR;

        return hash(concatenated);
    }


    private static String hash(String input) {
        int hash = 7;

        for (int i = 0; i < input.length(); i++) {
            // We explicitly calculate the modulo to fit into the 3 chars AND to always get a non-negative number
            // Why 31? See:
            //  https://stackoverflow.com/questions/299304/why-does-javas-hashcode-in-string-use-31-as-a-multiplier
            hash = (hash*31 + input.charAt(i)) % PRIME;
        }

        return int2string(hash);
    }


    // Convert the number into base 37 and pad it to 3 chars.
    // Note: The numbers are written from the smallest exponent to the biggest. But it does not matter.
    // Example:
    //  int2string(118) = "037" // Because 3*37 + 7 = 118
    //  int2string(1918) = "1EV" // Because 1*37*37 + 14*37 + 31  = 1918
    private static String int2string(int input) {
        String result = "";

        for (int i = 0; i < 3; i++) {
            int remainder =  input % ALPHABET.length;
            result = ALPHABET[remainder] + result;
            input = (input - remainder) / ALPHABET.length;
        }

        return result;
    }
}
