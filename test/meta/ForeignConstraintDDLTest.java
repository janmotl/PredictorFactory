package meta;

import org.junit.Test;

import java.util.List;

import static meta.ForeignConstraintDDL.extract;
import static meta.ForeignConstraintDDL.trimQuotes;
import static org.junit.Assert.assertEquals;

public class ForeignConstraintDDLTest {

	@Test
	public void readRecords_zero() throws Exception {
		String input = "course1\ncourse2\ncourse3";

		List<ForeignConstraint> obtained = extract(input);

		assertEquals(0, obtained.size());
	}

	@Test
	public void readRecords_one() throws Exception {
		String input =
				"ALTER TABLE order_products ADD CONSTRAINT name FOREIGN KEY (order_id)\n" +
				"    REFERENCES orders ( \"id\" )\n" +
				"NOT DEFERRABLE;";

		List<ForeignConstraint> obtained = extract(input);

		assertEquals(1, obtained.size());
		assertEquals("order_products", obtained.get(0).fTable);
		assertEquals("name", obtained.get(0).name);
		assertEquals("order_id", obtained.get(0).fColumn.get(0));
		assertEquals("orders", obtained.get(0).table);
		assertEquals("id", obtained.get(0).column.get(0));
	}

	@Test
	public void readRecords_multiple() throws Exception {
		String input =
				"ALTER TABLE fTable ADD CONSTRAINT name0 FOREIGN KEY (fColumn) REFERENCES table (fColumn);\n" +
				"ALTER TABLE fTable ADD CONSTRAINT name1 FOREIGN KEY (fColumn) REFERENCES table (fColumn);\n" +
				"ALTER TABLE fTable ADD CONSTRAINT name2 FOREIGN KEY (fColumn) REFERENCES table (fColumn);";

		List<ForeignConstraint> obtained = extract(input);

		assertEquals(3, obtained.size());
		assertEquals("name0", obtained.get(0).name);
		assertEquals("name1", obtained.get(1).name);
		assertEquals("name2", obtained.get(2).name);
	}

	@Test
	public void readRecords_compound() throws Exception {
		String input = "ALTER TABLE products\n" +
				"ADD CONSTRAINT fk_supplier\n" +
				"  FOREIGN KEY (supplier_id1, supplier_name1)\n" +
				"  REFERENCES supplier(supplier_id2, supplier_name2);";

		List<ForeignConstraint> obtained = extract(input);

		assertEquals(1, obtained.size());
		assertEquals("supplier_id1", obtained.get(0).fColumn.get(0));
		assertEquals("supplier_id2", obtained.get(0).column.get(0));
		assertEquals("supplier_name1", obtained.get(0).fColumn.get(1));
		assertEquals("supplier_name2", obtained.get(0).column.get(1));
	}


	@Test
	public void readRecords_noName() throws Exception {
		String input = "ALTER TABLE fTable ADD FOREIGN KEY (fColumn) REFERENCES table(fColumn);";

		List<ForeignConstraint> obtained = extract(input);

		assertEquals(1, obtained.size());
	}

	@Test
	public void readRecords_mysqWithSpaces() throws Exception {
		String input = "  ALTER  TABLE  products  ADD  CONSTRAINT  fk_supplier  FOREIGN  KEY  (`supplier_id`)  REFERENCES  supplier  (`supplier_id`)  ;  ";

		List<ForeignConstraint> obtained = extract(input);

		assertEquals(1, obtained.size());
	}

	@Test
	public void readRecords_mixedCaps() throws Exception {
		String input = "AlteR tABLE tab1 AdD FoREIGN  KeY (col1) References tab2 (col2);";

		List<ForeignConstraint> obtained = extract(input);

		assertEquals(1, obtained.size());
	}

	@Test
	public void readRecords_uglyNames() throws Exception {
		String input = "ALTER TABLE hr.hire_date ADD FOREIGN KEY (\"EVEN THIS & THAT!\") REFERENCES database.schema.table (a_very_long_and_valid_name);";

		List<ForeignConstraint> obtained = extract(input);

		assertEquals(1, obtained.size());
		assertEquals("hr.hire_date", obtained.get(0).fTable);
		assertEquals("EVEN THIS & THAT!", obtained.get(0).fColumn.get(0));
		assertEquals("database.schema.table", obtained.get(0).table);
		assertEquals("a_very_long_and_valid_name", obtained.get(0).column.get(0));
	}

	@Test
	public void readRecords_realExample_Instacart() throws Exception {
		String input = "\n" +
			"\n" +
			"\n" +
			"\n" +
			"CREATE TABLE aisles (\n" +
			"    \"aisle_id\"   INTEGER NOT NULL,\n" +
			"    \"aisle\"      VARCHAR2 \n" +
			");\n" +
			"\n" +
			"ALTER TABLE aisles ADD CONSTRAINT aisles_pk PRIMARY KEY ( \"aisle_id\" );\n" +
			"\n" +
			"CREATE TABLE departments (\n" +
			"    \"department_id\"   INTEGER NOT NULL,\n" +
			"    \"department\"      VARCHAR2 \n" +
			");\n" +
			"\n" +
			"ALTER TABLE departments ADD CONSTRAINT departments_pk PRIMARY KEY ( \"department_id\" );\n" +
			"\n" +
			"CREATE TABLE order_products__prior (\n" +
			"    \"order_id\"            INTEGER NOT NULL,\n" +
			"    \"product_id\"          INTEGER NOT NULL,\n" +
			"    \"add_to_cart_order\"   INTEGER,\n" +
			"    \"reordered\"           INTEGER\n" +
			");\n" +
			"\n" +
			"ALTER TABLE order_products__prior ADD CONSTRAINT order_products__prior_pk PRIMARY KEY ( \"product_id\" );\n" +
			"\n" +
			"CREATE TABLE order_products__train (\n" +
			"    \"order_id\"            INTEGER NOT NULL,\n" +
			"    \"product_id\"          INTEGER NOT NULL,\n" +
			"    \"add_to_cart_order\"   INTEGER,\n" +
			"    \"reordered\"           INTEGER,\n" +
			"    products_product_id   INTEGER NOT NULL,\n" +
			"    orders_order_id       INTEGER NOT NULL\n" +
			");\n" +
			"\n" +
			"ALTER TABLE order_products__train ADD CONSTRAINT order_products__train_pk PRIMARY KEY ( \"product_id\" );\n" +
			"\n" +
			"CREATE TABLE orders (\n" +
			"    \"order_id\"                 INTEGER NOT NULL,\n" +
			"    \"user_id\"                  INTEGER,\n" +
			"    \"eval_set\"                 VARCHAR2,\n" +
			"    \"order_number\"             INTEGER,\n" +
			"    \"order_dow\"                INTEGER,\n" +
			"    \"order_hour_of_day\"        INTEGER,\n" +
			"    \"days_since_prior_order\"   INTEGER\n" +
			");\n" +
			"\n" +
			"ALTER TABLE orders ADD CONSTRAINT orders_pk PRIMARY KEY ( \"order_id\" );\n" +
			"\n" +
			"CREATE TABLE products (\n" +
			"    \"product_id\"      INTEGER NOT NULL,\n" +
			"    \"product_name\"    VARCHAR2 \n" +
			"--  ERROR: VARCHAR2 size not specified \n" +
			"    ,\n" +
			"    \"aisle_id\"        INTEGER,\n" +
			"    \"department_id\"   INTEGER\n" +
			");\n" +
			"\n" +
			"ALTER TABLE products ADD CONSTRAINT products_pk PRIMARY KEY ( \"product_id\" );\n" +
			"\n" +
			"CREATE TABLE sample_submission (\n" +
			"    \"order_id\"   INTEGER NOT NULL,\n" +
			"    \"products\"   VARCHAR2 \n" +
			"--  ERROR: VARCHAR2 size not specified \n" +
			");\n" +
			"\n" +
			"ALTER TABLE sample_submission ADD CONSTRAINT sample_submission_pk PRIMARY KEY ( \"order_id\" );\n" +
			"\n" +
			"ALTER TABLE order_products__prior ADD CONSTRAINT order_products__prior_fk0 FOREIGN KEY ( \"order_id\" )\n" +
			"    REFERENCES orders ( \"order_id\" )\n" +
			"NOT DEFERRABLE;\n" +
			"\n" +
			"ALTER TABLE order_products__prior ADD CONSTRAINT order_products__prior_fk1 FOREIGN KEY ( \"product_id\" )\n" +
			"    REFERENCES products ( \"product_id\" )\n" +
			"NOT DEFERRABLE;\n" +
			"\n" +
			"ALTER TABLE order_products__train ADD CONSTRAINT order_products__train_orders_fk FOREIGN KEY ( orders_order_id )\n" +
			"    REFERENCES orders ( \"order_id\" )\n" +
			"NOT DEFERRABLE;\n" +
			"\n" +
			"ALTER TABLE order_products__train ADD CONSTRAINT order_products__train_products_fk FOREIGN KEY ( products_product_id )\n" +
			"    REFERENCES products ( \"product_id\" )\n" +
			"NOT DEFERRABLE;\n" +
			"\n" +
			"ALTER TABLE products ADD CONSTRAINT products_fk0 FOREIGN KEY ( \"aisle_id\" )\n" +
			"    REFERENCES aisles ( \"aisle_id\" )\n" +
			"NOT DEFERRABLE;\n" +
			"\n" +
			"ALTER TABLE products ADD CONSTRAINT products_fk1 FOREIGN KEY ( \"department_id\" )\n" +
			"    REFERENCES departments ( \"department_id\" )\n" +
			"NOT DEFERRABLE;\n" +
			"\n" +
			"ALTER TABLE sample_submission ADD CONSTRAINT sample_submission_fk0 FOREIGN KEY ( \"order_id\" )\n" +
			"    REFERENCES orders ( \"order_id\" )\n" +
			"NOT DEFERRABLE;\n" +
			"\n" +
			"\n" +
			"\n" +
			"-- Oracle SQL Developer Data Modeler Summary Report: \n" +
			"-- \n" +
			"-- CREATE TABLE                             7\n" +
			"-- CREATE INDEX                             0\n" +
			"-- ALTER TABLE                             14\n" +
			"-- CREATE VIEW                              0\n" +
			"-- ALTER VIEW                               0\n" +
			"-- CREATE PACKAGE                           0\n" +
			"-- CREATE PACKAGE BODY                      0\n" +
			"-- CREATE PROCEDURE                         0\n" +
			"-- CREATE FUNCTION                          0\n" +
			"-- CREATE TRIGGER                           0\n" +
			"-- ALTER TRIGGER                            0\n" +
			"-- CREATE COLLECTION TYPE                   0\n" +
			"-- CREATE STRUCTURED TYPE                   0\n" +
			"-- CREATE STRUCTURED TYPE BODY              0\n" +
			"-- CREATE CLUSTER                           0\n" +
			"-- CREATE CONTEXT                           0\n" +
			"-- CREATE DATABASE                          0\n" +
			"-- CREATE DIMENSION                         0\n" +
			"-- CREATE DIRECTORY                         0\n" +
			"-- CREATE DISK GROUP                        0\n" +
			"-- CREATE ROLE                              0\n" +
			"-- CREATE ROLLBACK SEGMENT                  0\n" +
			"-- CREATE SEQUENCE                          0\n" +
			"-- CREATE MATERIALIZED VIEW                 0\n" +
			"-- CREATE SYNONYM                           0\n" +
			"-- CREATE TABLESPACE                        0\n" +
			"-- CREATE USER                              0\n" +
			"-- \n" +
			"-- DROP TABLESPACE                          0\n" +
			"-- DROP DATABASE                            0\n" +
			"-- \n" +
			"-- REDACTION POLICY                         0\n" +
			"-- \n" +
			"-- ORDS DROP SCHEMA                         0\n" +
			"-- ORDS ENABLE SCHEMA                       0\n" +
			"-- ORDS ENABLE OBJECT                       0\n" +
			"-- \n" +
			"-- ERRORS                                   5\n" +
			"-- WARNINGS                                 0\n";

		List<ForeignConstraint> obtained = extract(input);

		assertEquals(7, obtained.size());
	}

	@Test
	public void readRecords_realExample_financial() throws Exception {
		String input = "SET CURRENT RULES = 'STD' ;\n" +
				"CREATE TABLE account \n" +
				"    ( \n" +
				"     account_id INTEGER NOT NULL , \n" +
				"     district_id INTEGER NOT NULL , \n" +
				"     frequency VARCHAR (18) NOT NULL , \n" +
				"     date DATE NOT NULL \n" +
				"    ) \n" +
				"    NOT VOLATILE \n" +
				";\n" +
				"CREATE INDEX district_id \n" +
				"    ON account \n" +
				"    ( \n" +
				"     district_id ASC \n" +
				"    ) \n" +
				"    DEFER NO \n" +
				";\n" +
				"\n" +
				"ALTER TABLE account ADD CONSTRAINT primary PRIMARY KEY ( account_id );\n" +
				"\n" +
				"SET CURRENT RULES = 'STD' ;\n" +
				"CREATE TABLE card \n" +
				"    ( \n" +
				"     card_id INTEGER NOT NULL , \n" +
				"     disp_id INTEGER NOT NULL , \n" +
				"     type VARCHAR (7) NOT NULL , \n" +
				"     issued DATE NOT NULL \n" +
				"    ) \n" +
				"    NOT VOLATILE \n" +
				";\n" +
				"CREATE INDEX disp_id \n" +
				"    ON card \n" +
				"    ( \n" +
				"     disp_id ASC \n" +
				"    ) \n" +
				"    DEFER NO \n" +
				";\n" +
				"\n" +
				"ALTER TABLE card ADD CONSTRAINT primary PRIMARY KEY ( card_id );\n" +
				"\n" +
				"SET CURRENT RULES = 'STD' ;\n" +
				"CREATE TABLE client \n" +
				"    ( \n" +
				"     client_id INTEGER NOT NULL , \n" +
				"     gender VARCHAR (1) NOT NULL , \n" +
				"     birth_date DATE NOT NULL , \n" +
				"     district_id INTEGER NOT NULL \n" +
				"    ) \n" +
				"    NOT VOLATILE \n" +
				";\n" +
				"CREATE INDEX district_idv1 \n" +
				"    ON client \n" +
				"    ( \n" +
				"     district_id ASC \n" +
				"    ) \n" +
				"    DEFER NO \n" +
				";\n" +
				"\n" +
				"ALTER TABLE client ADD CONSTRAINT primary PRIMARY KEY ( client_id );\n" +
				"\n" +
				"SET CURRENT RULES = 'STD' ;\n" +
				"CREATE TABLE disp \n" +
				"    ( \n" +
				"     disp_id INTEGER NOT NULL , \n" +
				"     client_id INTEGER NOT NULL , \n" +
				"     account_id INTEGER NOT NULL , \n" +
				"     type VARCHAR (9) NOT NULL \n" +
				"    ) \n" +
				"    NOT VOLATILE \n" +
				";\n" +
				"CREATE INDEX account_id \n" +
				"    ON disp \n" +
				"    ( \n" +
				"     account_id ASC \n" +
				"    ) \n" +
				"    DEFER NO \n" +
				";\n" +
				"CREATE INDEX client_id \n" +
				"    ON disp \n" +
				"    ( \n" +
				"     client_id ASC \n" +
				"    ) \n" +
				"    DEFER NO \n" +
				";\n" +
				"\n" +
				"ALTER TABLE disp ADD CONSTRAINT primary PRIMARY KEY ( disp_id );\n" +
				"\n" +
				"SET CURRENT RULES = 'STD' ;\n" +
				"CREATE TABLE district \n" +
				"    ( \n" +
				"     district_id INTEGER NOT NULL , \n" +
				"     A2 VARCHAR (19) NOT NULL , \n" +
				"     A3 VARCHAR (15) NOT NULL , \n" +
				"     A4 INTEGER NOT NULL , \n" +
				"     A5 INTEGER NOT NULL , \n" +
				"     A6 INTEGER NOT NULL , \n" +
				"     A7 INTEGER NOT NULL , \n" +
				"     A8 INTEGER NOT NULL , \n" +
				"     A9 INTEGER NOT NULL , \n" +
				"     A10 DECIMAL (4,1) NOT NULL , \n" +
				"     A11 INTEGER NOT NULL , \n" +
				"     A12 DECIMAL (4,1) , \n" +
				"     A13 DECIMAL (3,2) NOT NULL , \n" +
				"     A14 INTEGER NOT NULL , \n" +
				"     A15 INTEGER , \n" +
				"     A16 INTEGER NOT NULL \n" +
				"    ) \n" +
				"    NOT VOLATILE \n" +
				";\n" +
				"\n" +
				"ALTER TABLE district ADD CONSTRAINT primary PRIMARY KEY ( district_id );\n" +
				"\n" +
				"SET CURRENT RULES = 'STD' ;\n" +
				"CREATE TABLE loan \n" +
				"    ( \n" +
				"     loan_id INTEGER NOT NULL , \n" +
				"     account_id INTEGER NOT NULL , \n" +
				"     date DATE NOT NULL , \n" +
				"     amount INTEGER NOT NULL , \n" +
				"     duration INTEGER NOT NULL , \n" +
				"     payments DECIMAL (6,2) NOT NULL , \n" +
				"     status VARCHAR (1) NOT NULL \n" +
				"    ) \n" +
				"    NOT VOLATILE \n" +
				";\n" +
				"CREATE INDEX account_idv1 \n" +
				"    ON loan \n" +
				"    ( \n" +
				"     account_id ASC \n" +
				"    ) \n" +
				"    DEFER NO \n" +
				";\n" +
				"\n" +
				"ALTER TABLE loan ADD CONSTRAINT primary PRIMARY KEY ( loan_id );\n" +
				"\n" +
				"SET CURRENT RULES = 'STD' ;\n" +
				"CREATE TABLE \"order\" \n" +
				"    ( \n" +
				"     order_id INTEGER NOT NULL , \n" +
				"     account_id INTEGER NOT NULL , \n" +
				"     bank_to VARCHAR (2) NOT NULL , \n" +
				"     account_to INTEGER NOT NULL , \n" +
				"     amount DECIMAL (6,1) NOT NULL , \n" +
				"     k_symbol VARCHAR (8) NOT NULL \n" +
				"    ) \n" +
				"    NOT VOLATILE \n" +
				";\n" +
				"CREATE INDEX account_idv2 \n" +
				"    ON \"order\" \n" +
				"    ( \n" +
				"     account_id ASC \n" +
				"    ) \n" +
				"    DEFER NO \n" +
				";\n" +
				"\n" +
				"ALTER TABLE \"order\" ADD CONSTRAINT primary PRIMARY KEY ( order_id );\n" +
				"\n" +
				"SET CURRENT RULES = 'STD' ;\n" +
				"CREATE TABLE trans \n" +
				"    ( \n" +
				"     trans_id INTEGER NOT NULL , \n" +
				"     account_id INTEGER NOT NULL , \n" +
				"     date DATE NOT NULL , \n" +
				"     type VARCHAR (6) NOT NULL , \n" +
				"     operation VARCHAR (14) , \n" +
				"     amount INTEGER NOT NULL , \n" +
				"     balance INTEGER NOT NULL , \n" +
				"     k_symbol VARCHAR (11) , \n" +
				"     bank VARCHAR (2) , \n" +
				"     account INTEGER \n" +
				"    ) \n" +
				"    NOT VOLATILE \n" +
				";\n" +
				"CREATE INDEX account_idv3 \n" +
				"    ON trans \n" +
				"    ( \n" +
				"     account_id ASC \n" +
				"    ) \n" +
				"    DEFER NO \n" +
				";\n" +
				"\n" +
				"ALTER TABLE trans ADD CONSTRAINT primary PRIMARY KEY ( trans_id );\n" +
				"\n" +
				"ALTER TABLE account \n" +
				"    ADD CONSTRAINT account_ibfk_1 FOREIGN KEY \n" +
				"    ( \n" +
				"     district_id\n" +
				"    ) \n" +
				"    REFERENCES district \n" +
				"    ( \n" +
				"     district_id\n" +
				"    ) \n" +
				"    ON DELETE NO ACTION \n" +
				";\n" +
				"\n" +
				"ALTER TABLE card \n" +
				"    ADD CONSTRAINT card_ibfk_1 FOREIGN KEY \n" +
				"    ( \n" +
				"     disp_id\n" +
				"    ) \n" +
				"    REFERENCES disp \n" +
				"    ( \n" +
				"     disp_id\n" +
				"    ) \n" +
				"    ON DELETE NO ACTION \n" +
				";\n" +
				"\n" +
				"ALTER TABLE client \n" +
				"    ADD CONSTRAINT client_ibfk_1 FOREIGN KEY \n" +
				"    ( \n" +
				"     district_id\n" +
				"    ) \n" +
				"    REFERENCES district \n" +
				"    ( \n" +
				"     district_id\n" +
				"    ) \n" +
				"    ON DELETE NO ACTION \n" +
				";\n" +
				"\n" +
				"ALTER TABLE disp \n" +
				"    ADD CONSTRAINT disp_ibfk_1 FOREIGN KEY \n" +
				"    ( \n" +
				"     account_id\n" +
				"    ) \n" +
				"    REFERENCES account \n" +
				"    ( \n" +
				"     account_id\n" +
				"    ) \n" +
				"    ON DELETE NO ACTION \n" +
				";\n" +
				"\n" +
				"ALTER TABLE disp \n" +
				"    ADD CONSTRAINT disp_ibfk_2 FOREIGN KEY \n" +
				"    ( \n" +
				"     client_id\n" +
				"    ) \n" +
				"    REFERENCES client \n" +
				"    ( \n" +
				"     client_id\n" +
				"    ) \n" +
				"    ON DELETE NO ACTION \n" +
				";\n" +
				"\n" +
				"ALTER TABLE loan \n" +
				"    ADD CONSTRAINT loan_ibfk_1 FOREIGN KEY \n" +
				"    ( \n" +
				"     account_id\n" +
				"    ) \n" +
				"    REFERENCES account \n" +
				"    ( \n" +
				"     account_id\n" +
				"    ) \n" +
				"    ON DELETE NO ACTION \n" +
				";\n" +
				"\n" +
				"ALTER TABLE \"order\" \n" +
				"    ADD CONSTRAINT order_ibfk_1 FOREIGN KEY \n" +
				"    ( \n" +
				"     account_id\n" +
				"    ) \n" +
				"    REFERENCES account \n" +
				"    ( \n" +
				"     account_id\n" +
				"    ) \n" +
				"    ON DELETE NO ACTION \n" +
				";\n" +
				"\n" +
				"ALTER TABLE trans \n" +
				"    ADD CONSTRAINT trans_ibfk_1 FOREIGN KEY \n" +
				"    ( \n" +
				"     account_id\n" +
				"    ) \n" +
				"    REFERENCES account \n" +
				"    ( \n" +
				"     account_id\n" +
				"    ) \n" +
				"    ON DELETE NO ACTION \n" +
				";";

		List<ForeignConstraint> obtained = extract(input);

		assertEquals(8, obtained.size());
	}


	@Test
	public void removeQuotesBattery() throws Exception {
		assertEquals("apple", trimQuotes("apple"));
		assertEquals("pear", trimQuotes("pear"));
		assertEquals("apple", trimQuotes("\"apple\""));
		assertEquals("pear", trimQuotes("\"pear\""));
		assertEquals("apple pear", trimQuotes("\"apple pear\""));
		assertEquals("quote \" inside", trimQuotes("\"quote \" inside\""));
		assertEquals("apple", trimQuotes("`apple`"));
		assertEquals("`apple`", trimQuotes("\"`apple`\""));
	}


}