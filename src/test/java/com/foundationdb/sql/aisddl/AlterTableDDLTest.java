/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.aisddl;

import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.JoinColumn;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.ais.util.TableChange;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.api.ddl.DDLFunctionsMockBase;
import com.foundationdb.server.error.ColumnAlreadyGeneratedException;
import com.foundationdb.server.error.ColumnNotGeneratedException;
import com.foundationdb.server.error.DuplicateColumnNameException;
import com.foundationdb.server.error.DuplicateIndexException;
import com.foundationdb.server.error.JoinColumnMismatchException;
import com.foundationdb.server.error.JoinToMultipleParentsException;
import com.foundationdb.server.error.JoinToUnknownTableException;
import com.foundationdb.server.error.NoSuchColumnException;
import com.foundationdb.server.error.NoSuchConstraintException;
import com.foundationdb.server.error.NoSuchGroupingFKException;
import com.foundationdb.server.error.NoSuchSequenceException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.error.NoSuchUniqueException;
import com.foundationdb.server.error.ProtectedColumnDDLException;
import com.foundationdb.server.error.UnsupportedCheckConstraintException;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.AlterTableNode;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.SQLParserException;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.util.Strings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.foundationdb.ais.util.TableChangeValidator.ChangeLevel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class AlterTableDDLTest {
    private static final String SCHEMA = "test";
    private static final TableName C_NAME = tn(SCHEMA, "c");
    private static final TableName O_NAME = tn(SCHEMA, "o");
    private static final TableName I_NAME = tn(SCHEMA, "i");
    private static final TableName A_NAME = tn(SCHEMA, "a");

    private SQLParser parser;
    private DDLFunctionsMock ddlFunctions;
    private NewAISBuilder builder;

    @Before
    public void before() {
        parser = new SQLParser();
        builder = AISBBasedBuilder.create(MTypesTranslator.INSTANCE);
        ddlFunctions = null;
    }

    @After
    public void after() {
        parser = null;
        builder = null;
        ddlFunctions = null;
    }

    //
    // Assume check is done early, don't confirm for every action
    //

    @Test(expected=NoSuchTableException.class)
    public void cannotAlterUnknownTable() throws StandardException {
        parseAndRun("ALTER TABLE foo ADD COLUMN bar INT");
    }

    //
    // ADD COLUMN
    //

    @Test(expected=DuplicateColumnNameException.class)
    public void cannotAddDuplicateColumnName() throws StandardException {
        builder.table(A_NAME).colBigInt("aid", true);
        parseAndRun("ALTER TABLE a ADD COLUMN aid INT");
    }

    @Test
    public void addMultipleColumns() throws StandardException
    {
        builder.table(A_NAME).colBigInt("b", false);
        parseAndRun("ALTER TABLE a ADD COLUMN d INT, e INT");
        expectColumnChanges("ADD:d", "ADD:e");
        expectFinalTable(A_NAME, "b MCOMPAT_ BIGINT(21) NOT NULL",
                                 "d MCOMPAT_ INT(11) NULL",
                                 "e MCOMPAT_ INT(11) NULL");
    }
    
    @Test
    public void addColumnSingleTableGroupNoPK() throws StandardException {
        builder.table(A_NAME).colBigInt("aid", false);
        parseAndRun("ALTER TABLE a ADD COLUMN x INT");
        expectColumnChanges("ADD:x");
        expectIndexChanges();
        expectFinalTable(A_NAME, "aid MCOMPAT_ BIGINT(21) NOT NULL", "x MCOMPAT_ INT(11) NULL");
    }

    @Test
    public void addColumnSingleTableGroup() throws StandardException {
        builder.table(A_NAME).colBigInt("aid", false).pk("aid");
        parseAndRun("ALTER TABLE a ADD COLUMN v1 VARCHAR(32)");
        expectColumnChanges("ADD:v1");
        expectIndexChanges();
        expectFinalTable(A_NAME, "aid MCOMPAT_ BIGINT(21) NOT NULL", "v1 MCOMPAT_ VARCHAR(32", "UTF8", "UCS_BINARY) NULL", "PRIMARY(aid)");
    }

    @Test
    public void addNotNullColumnSingleTableGroup() throws StandardException {
        builder.table(A_NAME).colBigInt("aid", false).pk("aid");
        parseAndRun("ALTER TABLE a ADD COLUMN x INT NOT NULL DEFAULT 0");
        expectColumnChanges("ADD:x");
        expectIndexChanges();
        expectFinalTable(A_NAME, "aid MCOMPAT_ BIGINT(21) NOT NULL", "x MCOMPAT_ INT(11) NOT NULL DEFAULT 0", "PRIMARY(aid)");
    }

    @Test
    public void addColumnRootOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE c ADD COLUMN d1 DECIMAL(10,3)");
        expectColumnChanges("ADD:d1");
        expectIndexChanges();
        expectFinalTable(C_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL", "c_c MCOMPAT_ BIGINT(21) NULL", "d1 MCOMPAT_ DECIMAL(10, 3) NULL", "PRIMARY(id)");
        expectUnchangedTables(O_NAME, I_NAME, A_NAME);
    }

    @Test
    public void addColumnMiddleOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE o ADD COLUMN f1 real");
        expectColumnChanges("ADD:f1");
        expectIndexChanges();
        expectFinalTable(O_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL", "cid MCOMPAT_ BIGINT(21) NULL", "o_o MCOMPAT_ BIGINT(21) NULL",
                                 "f1 MCOMPAT_ FLOAT(-1, -1) NULL", "fk1(cid)", "PRIMARY(id)", "join(cid->id)");
        expectUnchangedTables(C_NAME, I_NAME, A_NAME);
    }

    @Test
    public void addColumnLeafOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE i ADD COLUMN d1 double");
        expectColumnChanges("ADD:d1");
        expectIndexChanges();
        expectFinalTable(I_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL", "oid MCOMPAT_ BIGINT(21) NULL",
                                 "i_i MCOMPAT_ BIGINT(21) NULL", "d1 MCOMPAT_ DOUBLE(-1, -1) NULL", "fk2(oid)",
                                 "PRIMARY(id)", "join(oid->id)");
        expectUnchangedTables(C_NAME, O_NAME, A_NAME);
    }
    
    @Test
    public void addColumnSerialNoPk() throws StandardException {
        builder.table(A_NAME).colBigInt("aid", false);
        parseAndRun("ALTER TABLE a ADD COLUMN new SERIAL");
        expectColumnChanges("ADD:new");
        expectIndexChanges();
        expectFinalTable(A_NAME, "aid MCOMPAT_ BIGINT(21) NOT NULL", "new MCOMPAT_ INT(11) NOT NULL GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1)");
    }
    
    @Test
    public void addColumnSerialPk() throws StandardException {
        builder.table(A_NAME).colBigInt("aid", false);
        parseAndRun("ALTER TABLE a ADD COLUMN new SERIAL PRIMARY KEY");
        expectColumnChanges("DROP:__akiban_pk", "ADD:new");
        expectIndexChanges("ADD:PRIMARY");
        expectFinalTable(A_NAME, "aid MCOMPAT_ BIGINT(21) NOT NULL", "new MCOMPAT_ INT(11) NOT NULL GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1)", "PRIMARY(new)");
    }

    //
    // DROP COLUMN
    //

    @Test(expected=NoSuchColumnException.class)
    public void cannotDropColumnUnknownColumn() throws StandardException {
        builder.table(A_NAME).colBigInt("aid", true);
        parseAndRun("ALTER TABLE a DROP COLUMN bar");
    }

    @Test
    public void dropColumnPKColumn() throws StandardException {
        builder.table(A_NAME).colBigInt("aid", false).colBigInt("x", true).pk("aid");
        parseAndRun("ALTER TABLE a DROP COLUMN aid");
        expectColumnChanges("DROP:aid", "ADD:__akiban_pk");
        expectFinalTable(A_NAME, "x MCOMPAT_ BIGINT(21) NULL");
    }

    @Test
    public void dropColumnSingleTableGroupNoPK() throws StandardException {
        builder.table(A_NAME).colBigInt("aid", false).colBigInt("x");
        parseAndRun("ALTER TABLE a DROP COLUMN x");
        expectColumnChanges("DROP:x");
        expectIndexChanges();
        expectFinalTable(A_NAME, "aid MCOMPAT_ BIGINT(21) NOT NULL");
    }

    @Test
    public void dropColumnSingleTableGroup() throws StandardException {
        builder.table(A_NAME).colBigInt("aid", false).colString("v1", 32).pk("aid");
        parseAndRun("ALTER TABLE a DROP COLUMN v1");
        expectColumnChanges("DROP:v1");
        expectIndexChanges();
        expectFinalTable(A_NAME, "aid MCOMPAT_ BIGINT(21) NOT NULL", "PRIMARY(aid)");
    }

    @Test
    public void dropColumnRootOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE c DROP COLUMN c_c");
        expectColumnChanges("DROP:c_c");
        expectIndexChanges();
        expectFinalTable(C_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL", "PRIMARY(id)");
        expectUnchangedTables(O_NAME, I_NAME, A_NAME);
    }

    @Test
    public void dropColumnMiddleOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE o DROP COLUMN o_o");
        expectColumnChanges("DROP:o_o");
        expectIndexChanges();
        expectFinalTable(O_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL", "cid MCOMPAT_ BIGINT(21) NULL",
                                 "fk1(cid)", "PRIMARY(id)", "join(cid->id)");
        expectUnchangedTables(C_NAME, I_NAME, A_NAME);
    }

    @Test
    public void dropColumnLeafOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE i DROP COLUMN i_i");
        expectColumnChanges("DROP:i_i");
        expectIndexChanges();
        expectFinalTable(I_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL", "oid MCOMPAT_ BIGINT(21) NULL",
                                 "fk2(oid)", "PRIMARY(id)", "join(oid->id)");
        expectUnchangedTables(C_NAME, O_NAME, A_NAME);
    }

    @Test
    public void dropColumnWasIndexed() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).colString("c1", 10).pk("id").key("c1", "c1");
        parseAndRun("ALTER TABLE c DROP COLUMN c1");
        expectColumnChanges("DROP:c1");
        expectFinalTable(C_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL", "PRIMARY(id)");
    }

    @Test
    public void dropColumnWasInMultiIndexed() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).colBigInt("c1", true).colBigInt("c2", true).pk("id").key("c1_c2", "c1", "c2");
        parseAndRun("ALTER TABLE c DROP COLUMN c1");
        expectColumnChanges("DROP:c1");
        expectFinalTable(C_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL", "c2 MCOMPAT_ BIGINT(21) NULL", "c1_c2(c2)", "PRIMARY(id)");
    }

    @Test
    public void dropColumnFromChildIsGroupedToParent() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE i DROP COLUMN oid");
        expectColumnChanges("DROP:oid");
        // Do not check group and assume join removal handled at lower level (TableChangeValidator)
        expectFinalTable(I_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL", "i_i MCOMPAT_ BIGINT(21) NULL", "PRIMARY(id)", "join(oid->id)");
    }

    //
    // ALTER COLUMN <metadata>
    //

    @Test
    public void alterColumnSetDefault() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", true);
        builder.unvalidatedAIS().getTable(C_NAME).getColumn("c1").setDefaultValue(null);
        parseAndRun("ALTER TABLE c ALTER COLUMN c1 SET DEFAULT 42");
        expectColumnChanges("MODIFY:c1->c1");
        expectIndexChanges();
        expectFinalTable(C_NAME, "c1 MCOMPAT_ BIGINT(21) NULL DEFAULT 42");
    }

    @Test
    public void alterColumnDropDefault() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", true);
        builder.unvalidatedAIS().getTable(C_NAME).getColumn("c1").setDefaultValue("42");
        parseAndRun("ALTER TABLE c ALTER COLUMN c1 DROP DEFAULT");
        expectColumnChanges("MODIFY:c1->c1");
        expectIndexChanges();
        expectFinalTable(C_NAME, "c1 MCOMPAT_ BIGINT(21) NULL");
    }

    @Test
    public void alterColumnNull() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false);
        parseAndRun("ALTER TABLE c ALTER COLUMN c1 NULL");
        expectColumnChanges("MODIFY:c1->c1");
        expectIndexChanges();
        expectFinalTable(C_NAME, "c1 MCOMPAT_ BIGINT(21) NULL");
    }

    @Test
    public void alterColumnNotNull() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false);
        parseAndRun("ALTER TABLE c ALTER COLUMN c1 NOT NULL");
        expectColumnChanges("MODIFY:c1->c1");
        expectIndexChanges();
        expectFinalTable(C_NAME, "c1 MCOMPAT_ BIGINT(21) NOT NULL");
    }

    @Test
    public void renameColumn() throws StandardException
    {   
        builder.table(C_NAME).colBigInt("a", true)
                                 .colBigInt("b", true)
                                 .colBigInt("x", false)
                                 .colBigInt("d", true)
                                 .pk("x")
                                 .key("idx1", "b", "x");
        
        parseAndRun("RENAME COLUMN c.x TO y");
        expectColumnChanges("MODIFY:x->y");
        expectIndexChanges();
        expectFinalTable(C_NAME,
                         "a MCOMPAT_ BIGINT(21) NULL, " +
                            "b MCOMPAT_ BIGINT(21) NULL, " +
                            "y MCOMPAT_ BIGINT(21) NOT NULL, " +
                            "d MCOMPAT_ BIGINT(21) NULL",
                         "idx1(b,y)",
                         "PRIMARY(y)");
    }

    //
    // ALTER COLUMN SET DATA TYPE
    //

    @Test(expected=NoSuchColumnException.class)
    public void cannotAlterColumnUnknownColumn() throws StandardException {
        builder.table(A_NAME).colBigInt("aid", true);
        parseAndRun("ALTER TABLE a ALTER COLUMN bar SET DATA TYPE INT");
    }

    @Test
    public void alterColumnFromChildIsGroupedToParent() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE i ALTER COLUMN oid SET DATA TYPE varchar(32)");
        expectColumnChanges("MODIFY:oid->oid");
        expectIndexChanges();
        // Do not check group and assume join removal handled at lower level (TableChangeValidator)
        expectFinalTable(I_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL", "oid MCOMPAT_ VARCHAR(32, UTF8, UCS_BINARY) NULL",
                                 "i_i MCOMPAT_ BIGINT(21) NULL", "fk2(oid)", "PRIMARY(id)", "join(oid->id)");
    }

    @Test
    public void alterColumnPKColumnSingleTableGroup() throws StandardException {
        builder.table(A_NAME).colBigInt("aid", false).pk("aid");
        parseAndRun("ALTER TABLE a ALTER COLUMN aid SET DATA TYPE INT");
        expectColumnChanges("MODIFY:aid->aid");
        expectIndexChanges();
        expectFinalTable(A_NAME, "aid MCOMPAT_ INT(11) NOT NULL", "PRIMARY(aid)");
    }

    @Test
    public void alterColumnSetDataTypeSingleTableGroupNoPK() throws StandardException {
        builder.table(A_NAME).colBigInt("aid", false).colBigInt("x");
        parseAndRun("ALTER TABLE a ALTER COLUMN x SET DATA TYPE varchar(32)");
        expectColumnChanges("MODIFY:x->x");
        expectIndexChanges();
        expectFinalTable(A_NAME, "aid MCOMPAT_ BIGINT(21) NOT NULL", "x MCOMPAT_ VARCHAR(32, UTF8, UCS_BINARY) NOT NULL");
    }

    @Test
    public void alterColumnSetDataTypeSingleTableGroup() throws StandardException {
        builder.table(A_NAME).colBigInt("aid", false).colString("v1", 32, true).pk("aid");
        parseAndRun("ALTER TABLE a ALTER COLUMN v1 SET DATA TYPE INT");
        expectColumnChanges("MODIFY:v1->v1");
        expectIndexChanges();
        expectFinalTable(A_NAME, "aid MCOMPAT_ BIGINT(21) NOT NULL", "v1 MCOMPAT_ INT(11) NULL", "PRIMARY(aid)");
    }

    @Test
    public void alterColumnSetDataTypeRootOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE c ALTER COLUMN c_c SET DATA TYPE DECIMAL(5,2)");
        expectColumnChanges("MODIFY:c_c->c_c");
        expectIndexChanges();
        expectFinalTable(C_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL", "c_c MCOMPAT_ DECIMAL(5, 2) NULL", "PRIMARY(id)");
        expectUnchangedTables(O_NAME, I_NAME, A_NAME);
    }

    @Test
    public void alterColumnSetDataTypeMiddleOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE o ALTER COLUMN o_o SET DATA TYPE varchar(10)");
        expectColumnChanges("MODIFY:o_o->o_o");
        expectIndexChanges();
        expectFinalTable(O_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL", "cid MCOMPAT_ BIGINT(21) NULL",
                                 "o_o MCOMPAT_ VARCHAR(10, UTF8, UCS_BINARY) NULL", "fk1(cid)",
                                 "PRIMARY(id)", "join(cid->id)");
        expectUnchangedTables(C_NAME, I_NAME, A_NAME);
    }

    @Test
    public void alterColumnSetDataTypeLeafOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE i ALTER COLUMN i_i SET DATA TYPE double");
        expectColumnChanges("MODIFY:i_i->i_i");
        expectIndexChanges();
        expectFinalTable(I_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL", "oid MCOMPAT_ BIGINT(21) NULL",
                                 "i_i MCOMPAT_ DOUBLE(-1, -1) NULL", "fk2(oid)", "PRIMARY(id)", "join(oid->id)");
        expectUnchangedTables(C_NAME, O_NAME, A_NAME);
    }

    //
    // ALTER COLUMN DROP DEFAULT (where default is generated)
    //
    @Test
    public void alterColumnDropDefaultGenerated() throws StandardException {
        buildCWithGeneratedID(1, true);
        parseAndRun("ALTER TABLE c ALTER COLUMN id DROP DEFAULT");
        expectColumnChanges("MODIFY:id->id");
        expectIndexChanges();
        expectFinalTable(C_NAME, "id MCOMPAT_ INT(11) NOT NULL", "PRIMARY(id)");
    }

    //
    // ALTER COLUMN SET INCREMENT BY <number>
    //

    @Test(expected=UnsupportedSQLException.class)
    public void alterColumnSetIncrementByLess() throws StandardException {
        buildCWithGeneratedID(1, true);
        parseAndRun("ALTER TABLE c ALTER COLUMN id SET INCREMENT BY -1");
        expectColumnChanges("MODIFY:id->id");
        expectIndexChanges();
        expectFinalTable(C_NAME, "id MCOMPAT_ INT(11) NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY -1)", "PRIMARY(id)");
    }

    @Test(expected=UnsupportedSQLException.class)
    public void alterColumnSetIncrementByMore() throws StandardException {
        buildCWithGeneratedID(1, true);
        parseAndRun("ALTER TABLE c ALTER COLUMN id SET INCREMENT BY 5");
        expectColumnChanges("MODIFY:id->id");
        expectIndexChanges();
        expectFinalTable(C_NAME, "id MCOMPAT_ INT(11) NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 5)", "PRIMARY(id)");
    }

    @Test(expected=UnsupportedSQLException.class)
    public void alterColumnSetIncrementInvalid() throws StandardException {
        buildCWithID();
        parseAndRun("ALTER TABLE c ALTER COLUMN id SET INCREMENT BY 5");
    }

    //
    // ALTER COLUMN RESTART WITH <number>
    //

    @Test
    public void alterColumnRestartWith() throws StandardException {
        buildCWithGeneratedID(1, true);
        parseAndRun("ALTER TABLE c ALTER COLUMN id RESTART WITH 42");
        assertEquals("Sequence(test.temp-seq-c-id,42,1,1,9223372036854775807,false)", ddlFunctions.newSeqDesc);
    }

    //
    // ALTER COLUMN [SET] GENERATED <BY DEFAULT | ALWAYS>
    //

    @Test
    public void alterColumnSetGeneratedByDefault() throws StandardException {
        buildCWithID();
        parseAndRun("ALTER TABLE c ALTER COLUMN id SET GENERATED BY DEFAULT AS IDENTITY (START WITH 10, INCREMENT BY 50)");
        expectColumnChanges("MODIFY:id->id");
        expectIndexChanges();
        expectFinalTable(C_NAME, "id MCOMPAT_ INT(11) NOT NULL GENERATED BY DEFAULT AS IDENTITY (START WITH 10, INCREMENT BY 50)", "PRIMARY(id)");
    }

    @Test
    public void alterColumnSetGeneratedAlways() throws StandardException {
        buildCWithID();
        parseAndRun("ALTER TABLE c ALTER COLUMN id SET GENERATED ALWAYS AS IDENTITY (START WITH 42, INCREMENT BY 100)");
        expectColumnChanges("MODIFY:id->id");
        expectIndexChanges();
        expectFinalTable(C_NAME, "id MCOMPAT_ INT(11) NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 42, INCREMENT BY 100)", "PRIMARY(id)");
    }

    @Test(expected=ColumnAlreadyGeneratedException.class)
    public void alterColumnSetGeneratedAlreadyGenerated() throws StandardException {
        buildCWithGeneratedID(1, true);
        parseAndRun("ALTER TABLE c ALTER COLUMN id SET GENERATED ALWAYS AS IDENTITY (START WITH 42, INCREMENT BY 100)");
    }

    //
    // ADD [CONSTRAINT] UNIQUE
    //

    @Test(expected=NoSuchColumnException.class)
    public void cannotAddUniqueUnknownColumn() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false);
        parseAndRun("ALTER TABLE c ADD UNIQUE(c2)");
    }

    @Test
    public void addUniqueUnnamedSingleTableGroupNoPK() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false);
        parseAndRun("ALTER TABLE c ADD UNIQUE(c1)");
        expectColumnChanges();
        expectIndexChanges("ADD:c1");
        expectFinalTable(C_NAME, "c1 MCOMPAT_ BIGINT(21) NOT NULL", "UNIQUE c1(c1)");
    }

    @Test
     public void addUniqueNamedSingleTableGroupNoPK() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false);
        parseAndRun("ALTER TABLE c ADD CONSTRAINT x UNIQUE(c1)");
        expectColumnChanges();
        expectIndexChanges("ADD:x");
        expectFinalTable(C_NAME, "c1 MCOMPAT_ BIGINT(21) NOT NULL", "UNIQUE x(c1)");
        Table c = ddlFunctions.ais.getTable(C_NAME);
        assertNotNull(c.getIndex("x"));
    }

    @Test
    public void addUniqueMiddleOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE o ADD UNIQUE(o_o)");
        expectColumnChanges();
        expectIndexChanges("ADD:o_o");
        expectFinalTable(O_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL", "cid MCOMPAT_ BIGINT(21) NULL", "o_o MCOMPAT_ BIGINT(21) NULL",
                        "fk1(cid)", "UNIQUE o_o(o_o)", "PRIMARY(id)", "join(cid->id)");
        expectUnchangedTables(C_NAME, I_NAME, A_NAME);
    }

    //
    // DROP UNIQUE
    //

    @Test(expected=NoSuchUniqueException.class)
    public void cannotDropUniqueUnknown() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false);
        parseAndRun("ALTER TABLE c DROP UNIQUE c1");
    }

    @Test
      public void dropUniqueSingleColumnSingleTableGroup() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false).uniqueKey("c1", "c1");
        parseAndRun("ALTER TABLE c DROP UNIQUE c1");
        expectColumnChanges();
        expectIndexChanges("DROP:c1");
        expectFinalTable(C_NAME, "c1 MCOMPAT_ BIGINT(21) NOT NULL");
    }

    @Test
    public void dropUniqueMultiColumnSingleTableGroup() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false).colBigInt("c2", false).uniqueKey("x", "c2", "c1");
        parseAndRun("ALTER TABLE c DROP UNIQUE x");
        expectColumnChanges();
        expectIndexChanges("DROP:x");
        expectFinalTable(C_NAME, "c1 MCOMPAT_ BIGINT(21) NOT NULL", "c2 MCOMPAT_ BIGINT(21) NOT NULL");
    }

    @Test
    public void dropUniqueMiddleOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();
        AISBuilder builder2 = new AISBuilder(builder.unvalidatedAIS());
        builder2.unique(SCHEMA, "o", "x");
        builder2.indexColumn(SCHEMA, "o", "x", "o_o", 0, true, null);
        parseAndRun("ALTER TABLE o DROP UNIQUE x");
        expectColumnChanges();
        expectIndexChanges("DROP:x");
        expectFinalTable(O_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL", "cid MCOMPAT_ BIGINT(21) NULL", "o_o MCOMPAT_ BIGINT(21) NULL",
                         "fk1(cid)", "PRIMARY(id)", "join(cid->id)");
        expectUnchangedTables(C_NAME, I_NAME, A_NAME);
    }

    //
    // ADD [CONSTRAINT] PRIMARY KEY
    //

    @Test(expected=DuplicateIndexException.class)
    public void cannotAddPrimaryKeyAnotherPK() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false).pk("c1");
        parseAndRun("ALTER TABLE c ADD PRIMARY KEY(c1)");
    }

    //bug1047037
    @Test(expected=DuplicateIndexException.class)
    public void cannotAddNamedConstraintPrimaryKeyAnotherPK() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false).pk("c1");
        parseAndRun("ALTER TABLE c ADD CONSTRAINT break PRIMARY KEY(c1)");
    }

    @Test
    public void addPrimaryKeySingleTableGroupNoPK() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false);
        parseAndRun("ALTER TABLE c ADD PRIMARY KEY(c1)");
        expectColumnChanges("DROP:__akiban_pk", "MODIFY:c1->c1");
        expectIndexChanges("ADD:PRIMARY");
        expectFinalTable(C_NAME, "c1 MCOMPAT_ BIGINT(21) NOT NULL", "PRIMARY(c1)");
    }
    
    @Test
    public void addPrimaryKeyNullColumn() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", true);
        parseAndRun("ALTER TABLE c ADD PRIMARY KEY (c1)");
        expectColumnChanges("DROP:__akiban_pk","MODIFY:c1->c1");
        expectIndexChanges("ADD:PRIMARY");
        expectFinalTable(C_NAME,"c1 MCOMPAT_ BIGINT(21) NOT NULL", "PRIMARY(c1)");
    }
    
    @Test
    public void addPrimary2KeyFirstNullColumn() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", true).colString("c2", 32, false);
        parseAndRun("ALTER TABLE c ADD PRIMARY KEY (c1,c2)");
        expectColumnChanges("DROP:__akiban_pk", "MODIFY:c1->c1", "MODIFY:c2->c2");
        expectIndexChanges("ADD:PRIMARY");
        expectFinalTable(C_NAME,"c1 MCOMPAT_ BIGINT(21) NOT NULL", "c2 MCOMPAT_ VARCHAR(32, UTF8, UCS_BINARY) NOT NULL", "PRIMARY(c1,c2)");
    }

    @Test
    public void addPrimary2KeySecondNullColumn() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false).colString("c2", 32, true);
        parseAndRun("ALTER TABLE c ADD PRIMARY KEY (c1,c2)");
        expectColumnChanges("DROP:__akiban_pk", "MODIFY:c1->c1", "MODIFY:c2->c2");
        expectIndexChanges("ADD:PRIMARY");
        expectFinalTable(C_NAME,"c1 MCOMPAT_ BIGINT(21) NOT NULL", "c2 MCOMPAT_ VARCHAR(32, UTF8, UCS_BINARY) NOT NULL", "PRIMARY(c1,c2)");
    }

    @Test
    public void addPrimary2KeyBothNullColumn() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", true).colString("c2", 32, true);
        parseAndRun("ALTER TABLE c ADD PRIMARY KEY (c1,c2)");
        expectColumnChanges("DROP:__akiban_pk", "MODIFY:c1->c1", "MODIFY:c2->c2");
        expectIndexChanges("ADD:PRIMARY");
        expectFinalTable(C_NAME,"c1 MCOMPAT_ BIGINT(21) NOT NULL", "c2 MCOMPAT_ VARCHAR(32, UTF8, UCS_BINARY) NOT NULL", "PRIMARY(c1,c2)");
    }

    @Test
    public void addPrimaryKeyLeafTableTwoTableGroup() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).colBigInt("c_c", true).pk("id");
        builder.table(O_NAME).colBigInt("id", false).colBigInt("cid", true).joinTo(SCHEMA, "c", "fk").on("cid", "id");
        parseAndRun("ALTER TABLE o ADD PRIMARY KEY(id)");
        expectColumnChanges("DROP:__akiban_pk", "MODIFY:id->id");
        // Cascading changes due to PK (e.g. additional indexes) handled by lower layer
        expectIndexChanges("ADD:PRIMARY");
        expectFinalTable(O_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL", "cid MCOMPAT_ BIGINT(21) NULL", "fk(cid)", "PRIMARY(id)", "join(cid->id)");
        expectUnchangedTables(C_NAME);
    }

    //
    // DROP PRIMARY KEY
    //

    @Test(expected=NoSuchConstraintException.class)
    public void cannotDropPrimaryKeySingleTableGroupNoPK() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false);
        parseAndRun("ALTER TABLE c DROP PRIMARY KEY");
    }

    @Test(expected=NoSuchColumnException.class)
    public void cannotDropHiddenPrimaryKeyColumn() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false);
        parseAndRun("ALTER TABLE c DROP \"" + Column.AKIBAN_PK_NAME + "\"");
    }

    @Test(expected=ProtectedColumnDDLException.class)
    public void cannotAddHiddenPrimaryKeyColumn() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false).pk("c1");
        parseAndRun("ALTER TABLE c ADD COLUMN \"" + Column.AKIBAN_PK_NAME + "\" INT DEFAULT 3");
    }

    @Test(expected=NoSuchColumnException.class)
    public void cannotAlterHiddenPrimaryKeyColumn() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false);
        parseAndRun("ALTER TABLE c ALTER COLUMN \"" + Column.AKIBAN_PK_NAME + "\" SET DEFAULT 3");
    }

    @Test
    public void dropPrimaryKeySingleTableGroup() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false).pk("c1");
        parseAndRun("ALTER TABLE c DROP PRIMARY KEY");
        expectColumnChanges("ADD:__akiban_pk");
        expectIndexChanges("DROP:PRIMARY");
        expectFinalTable(C_NAME, "c1 MCOMPAT_ BIGINT(21) NOT NULL");
    }

    @Test
    public void dropPrimaryKeyLeafTableTwoTableGroup() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).colBigInt("c_c", true).pk("id");
        builder.table(O_NAME).colBigInt("id", false).colBigInt("cid", true).pk("id").joinTo(SCHEMA, "c", "fk").on(
                "cid", "id");
        parseAndRun("ALTER TABLE o DROP PRIMARY KEY");
        expectColumnChanges("ADD:__akiban_pk");
        // Cascading changes due to PK (e.g. additional indexes) handled by lower layer
        expectIndexChanges("DROP:PRIMARY");
        expectFinalTable(O_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL", "cid MCOMPAT_ BIGINT(21) NULL", "fk(cid)", "join(cid->id)");
        expectUnchangedTables(C_NAME);
    }

    @Test
    public void dropPrimaryKeyMiddleOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE o DROP PRIMARY KEY");
        expectColumnChanges("ADD:__akiban_pk");
        // Cascading changes due to PK (e.g. additional indexes) handled by lower layer
        expectIndexChanges("DROP:PRIMARY");
         expectFinalTable(O_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL", "cid MCOMPAT_ BIGINT(21) NULL", "o_o MCOMPAT_ BIGINT(21) NULL",
                          "fk1(cid)", "join(cid->id)");
        // Note: Cannot check I_NAME, grouping change propagated below AlterTableDDL layer
        expectUnchangedTables(C_NAME);
    }

    //
    // ADD [CONSTRAINT] CHECK
    //

    @Test(expected=UnsupportedCheckConstraintException.class)
    public void cannotAddCheckConstraint() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false);
        parseAndRun("ALTER TABLE c ADD CHECK (c1 % 5 = 0)");
    }

    //
    // DROP CHECK
    //

    @Test(expected=UnsupportedCheckConstraintException.class)
    public void cannotDropCheckConstraint() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false).uniqueKey("c1", "c1");
        parseAndRun("ALTER TABLE c DROP CHECK c1");
    }

    //
    // DROP CONSTRAINT
    //

    @Test(expected=NoSuchConstraintException.class)
    public void cannotDropConstraintRegularIndex() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false).key("c1", "c1");
        parseAndRun("ALTER TABLE c DROP CONSTRAINT c1");
    }

    @Test
    public void dropConstraintIsPrimaryKey() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false).pk("c1");
        parseAndRun("ALTER TABLE c DROP CONSTRAINT \"c_pkey\"");
        expectColumnChanges("ADD:__akiban_pk");
        expectIndexChanges("DROP:PRIMARY");
        expectFinalTable(C_NAME, "c1 MCOMPAT_ BIGINT(21) NOT NULL");
    }

    @Test
    public void dropIndexIsPrimaryKey() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false).pk("c1");
        parseAndRun("ALTER TABLE c DROP INDEX \"PRIMARY\"");
        expectColumnChanges("ADD:__akiban_pk");
        expectIndexChanges("DROP:PRIMARY");
        expectFinalTable(C_NAME, "c1 MCOMPAT_ BIGINT(21) NOT NULL");
    }
    
    @Test
    public void dropConstraintIsUnique() throws StandardException {
        builder.table(C_NAME).colBigInt("c1", false).uniqueConstraint("c1", "c1", "c1");
        parseAndRun("ALTER TABLE c DROP CONSTRAINT c1");
        expectFinalTable(C_NAME, "c1 MCOMPAT_ BIGINT(21) NOT NULL");
    }

    //
    // ADD [CONSTRAINT] GROUPING FOREIGN KEY
    //

    @Test(expected=JoinToUnknownTableException.class)
    public void cannotAddGFKToUnknownParent() throws StandardException {
        builder.table(C_NAME).colBigInt("cid", false).colBigInt("other").pk("cid");
        parseAndRun("ALTER TABLE c ADD GROUPING FOREIGN KEY(other) REFERENCES zap(id)");
    }

    @Test(expected=JoinToMultipleParentsException.class)
    public void cannotAddGFKToTableWithParent() throws StandardException {
        builder.table(C_NAME).colBigInt("cid", false).pk("cid");
        builder.table(O_NAME).colBigInt("oid", false).colBigInt("cid").pk("oid").joinTo(C_NAME).on("cid", "cid");
        parseAndRun("ALTER TABLE o ADD GROUPING FOREIGN KEY(cid) REFERENCES c(cid)");
    }

    @Test(expected=NoSuchColumnException.class)
    public void cannotAddGFKToUnknownParentColumns() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE a ADD GROUPING FOREIGN KEY(aid) REFERENCES c(banana)");
    }

    @Test(expected=NoSuchColumnException.class)
    public void cannotAddGFKToUnknownChildColumns() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE a ADD GROUPING FOREIGN KEY(banana) REFERENCES c(id)");
    }

    @Test(expected= JoinColumnMismatchException.class)
    public void cannotAddGFKToTooManyChildColumns() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).pk("id");
        builder.table(A_NAME).colBigInt("id", false).colBigInt("y").pk("id");
        parseAndRun("ALTER TABLE a ADD GROUPING FOREIGN KEY(id,y) REFERENCES c(id)");
    }

    @Test(expected=JoinColumnMismatchException.class)
    public void cannotAddGFKToTooManyParentColumns() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).colBigInt("x").pk("id");
        builder.table(A_NAME).colBigInt("id", false).colBigInt("y").pk("id");
        parseAndRun("ALTER TABLE a ADD GROUPING FOREIGN KEY(y) REFERENCES c(id,x)");
    }

    @Test
    public void dropGFKToTableWithChild() throws StandardException {
        builder.table(A_NAME).colBigInt("aid", false).pk("aid");
        builder.table(C_NAME).colBigInt("cid", false).colBigInt("aid").pk("cid");
        builder.table(O_NAME).colBigInt("oid", false).colBigInt("cid").pk("oid").joinTo(C_NAME).on("cid", "cid");
        parseAndRun("ALTER TABLE c ADD GROUPING FOREIGN KEY(aid) REFERENCES a(aid)");
        expectGroupIsSame(A_NAME, C_NAME, true);
        expectChildOf(A_NAME, C_NAME);
        expectChildOf(C_NAME, O_NAME);
    }

    @Test
    public void addGFKToSingleTableOnSingleTable() throws StandardException {
        builder.table(C_NAME).colBigInt("cid", false).pk("cid");
        builder.table(O_NAME).colBigInt("oid", false).colBigInt("cid").pk("oid");

        parseAndRun("ALTER TABLE o ADD GROUPING FOREIGN KEY(cid) REFERENCES c(cid)");

        expectGroupIsSame(C_NAME, O_NAME, true);
        expectChildOf(C_NAME, O_NAME);
    }

    @Test
    public void addGFKToPkLessTable() throws StandardException {
        builder.table(C_NAME).colBigInt("cid", false).pk("cid");
        builder.table(O_NAME).colBigInt("oid", false).colBigInt("cid");

        parseAndRun("ALTER TABLE o ADD GROUPING FOREIGN KEY(cid) REFERENCES c(cid)");

        expectGroupIsSame(C_NAME, O_NAME, true);
        expectChildOf(C_NAME, O_NAME);
    }

    @Test
    public void addGFKToSingleTableOnRootOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();

        parseAndRun("ALTER TABLE a ADD GROUPING FOREIGN KEY(other_id) REFERENCES c(id)");

        expectGroupIsSame(C_NAME, A_NAME, true);
        expectGroupIsSame(C_NAME, O_NAME, true);
        expectGroupIsSame(C_NAME, I_NAME, true);
        expectChildOf(C_NAME, A_NAME);
    }

    @Test
    public void addGFKToSingleTableOnMiddleOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();

        parseAndRun("ALTER TABLE a ADD GROUPING FOREIGN KEY(other_id) REFERENCES o(id)");

        expectGroupIsSame(C_NAME, A_NAME, true);
        expectGroupIsSame(C_NAME, O_NAME, true);
        expectGroupIsSame(C_NAME, I_NAME, true);
        expectChildOf(O_NAME, A_NAME);
    }

    @Test
    public void addGFKToSingleTableOnLeafOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();

        parseAndRun("ALTER TABLE a ADD GROUPING FOREIGN KEY(other_id) REFERENCES i(id)");

        expectGroupIsSame(C_NAME, A_NAME, true);
        expectGroupIsSame(C_NAME, O_NAME, true);
        expectGroupIsSame(C_NAME, I_NAME, true);
        expectChildOf(I_NAME, A_NAME);
    }

    @Test
    public void addGFKToTableDifferentSchema() throws StandardException {
        String schema2 = "foo";
        TableName xName = tn(schema2, "x");

        builder.table(C_NAME).colBigInt("id", false).pk("id");
        builder.table(xName).colBigInt("id", false).colBigInt("cid").pk("id");

        parseAndRun("ALTER TABLE foo.x ADD GROUPING FOREIGN KEY(cid) REFERENCES c(id)");

        expectGroupIsSame(C_NAME, xName, true);
        expectChildOf(C_NAME, xName);
    }

    // Should map automatically to the PK
    @Test
    public void addGFKWithNoReferencedSingleColumn() throws StandardException {
        buildCOIJoinedAUnJoined();

        parseAndRun("ALTER TABLE a ADD GROUPING FOREIGN KEY(other_id) REFERENCES c");

        expectGroupIsSame(C_NAME, A_NAME, true);
        expectChildOf(C_NAME, A_NAME);
    }

    @Test
    public void addGFKWithNoReferencedMultiColumn() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).colBigInt("id2", false).pk("id", "id2");
        builder.table(A_NAME).colBigInt("id", false).colBigInt("other_id").colBigInt("other_id2").pk("id");

        parseAndRun("ALTER TABLE a ADD GROUPING FOREIGN KEY(other_id,other_id2) REFERENCES c");

        expectGroupIsSame(C_NAME, A_NAME, true);
        expectChildOf(C_NAME, A_NAME);
    }

    @Test(expected=JoinColumnMismatchException.class)
    public void addGFKWithNoReferenceSingleColumnToMultiColumn() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).colBigInt("id2", false).pk("id","id2");
        builder.table(A_NAME).colBigInt("id", false).colBigInt("other_id").colBigInt("other_id2").pk("id");
        parseAndRun("ALTER TABLE a ADD GROUPING FOREIGN KEY(other_id) REFERENCES c");
    }

    @Test(expected=SQLParserException.class)
    public void addGFKReferencedColumnListCannotBeEmpty() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).colBigInt("id2", false).pk("id","id2");
        builder.table(A_NAME).colBigInt("id", false).colBigInt("other_id").colBigInt("other_id2").pk("id");
        parseAndRun("ALTER TABLE a ADD GROUPING FOREIGN KEY(other_id,other_id2) REFERENCES c()");
    }

    @Test
    public void addGFKNonDefaultSchema() throws StandardException {
        String otherSchema = SCHEMA + "2";
        builder.table(otherSchema, "p").colBigInt("pid", false).pk("pid");
        builder.table(otherSchema, "c").colBigInt("cid", false).colBigInt("pid", true).pk("cid");
        parseAndRun(String.format("ALTER TABLE %s.%s ADD GROUPING FOREIGN KEY(pid) REFERENCES %s.%s(pid)", otherSchema, "c", otherSchema, "p"));
        TableName pName = new TableName(otherSchema, "p");
        TableName cName = new TableName(otherSchema, "c");
        expectGroupIsSame(pName, cName, true);
        expectChildOf(pName, cName);
    }

    //
    // DROP [CONSTRAINT] GROUPING FOREIGN KEY
    //

    @Test(expected=NoSuchGroupingFKException.class)
    public void cannotDropGFKFromSingleTableGroup() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).pk("id");
        parseAndRun("ALTER TABLE c DROP GROUPING FOREIGN KEY");
    }

    @Test(expected=NoSuchGroupingFKException.class)
    public void cannotDropGFKFromRootOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE c DROP GROUPING FOREIGN KEY");
    }

    @Test
    public void dropGFKFromMiddleOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE o DROP GROUPING FOREIGN KEY");
        expectGroupIsSame(C_NAME, O_NAME, false);
        expectChildOf(O_NAME, I_NAME);
    }

    @Test
     public void dropGFKLeafFromTwoTableGroup() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).pk("id");
        builder.table(A_NAME).colBigInt("id", false).colBigInt("cid").pk("id").joinTo(C_NAME).on("cid", "id");
        parseAndRun("ALTER TABLE a DROP GROUPING FOREIGN KEY");
        expectGroupIsSame(C_NAME, A_NAME, false);
    }

    @Test
    public void dropGFKLeafFromGroup() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE i DROP GROUPING FOREIGN KEY");
        expectGroupIsSame(C_NAME, I_NAME, false);
    }

    @Test
    public void dropGFKLeafWithNoPKFromGroup() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).pk("id");
        builder.table(A_NAME).colBigInt("id", false).colBigInt("cid").joinTo(C_NAME).on("cid", "id");
        parseAndRun("ALTER TABLE a DROP GROUPING FOREIGN KEY");
        expectGroupIsSame(C_NAME, A_NAME, false);
    }

    @Test
    public void dropGFKFromCrossSchemaGroup() throws StandardException {
        String schema2 = "foo";
        TableName xName = tn(schema2, "x");
        builder.table(C_NAME).colBigInt("id", false).pk("id");
        builder.table(xName).colBigInt("id", false).colBigInt("cid").pk("id").joinTo(C_NAME).on("cid", "id");
        parseAndRun("ALTER TABLE foo.x DROP GROUPING FOREIGN KEY");
        expectGroupIsSame(C_NAME, xName, false);
    }

    @Test 
    public void dropGFKByName() throws StandardException {
        buildCOIJoinedAUnJoined();
        Table i = builder.ais().getTable(I_NAME);
        assertEquals (i.getParentJoin().getName(), "fk2");
        parseAndRun("ALTER TABLE i DROP constraint `fk2`");
        expectGroupIsSame(C_NAME, I_NAME, false);
    }

    //
    // ALTER GROUP ADD
    //

    @Test
    public void groupAddSimple() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER GROUP ADD TABLE a(other_id) TO c(id)");
        expectGroupIsSame(C_NAME, A_NAME, true);
        expectChildOf(C_NAME, A_NAME);
    }

    @Test
    public void groupAddNoReferencedSingleColumn() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER GROUP ADD TABLE a(other_id) TO c");
        expectGroupIsSame(C_NAME, A_NAME, true);
        expectChildOf(C_NAME, A_NAME);
    }

    @Test
    public void groupAddNoReferencedMultiColumn() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).colBigInt("id2", false).pk("id","id2");
        builder.table(A_NAME).colBigInt("id", false).colBigInt("other_id").colBigInt("other_id2").pk("id");
        parseAndRun("ALTER GROUP ADD TABLE a(other_id,other_id2) TO c");
        expectGroupIsSame(C_NAME, A_NAME, true);
        expectChildOf(C_NAME, A_NAME);
    }

    @Test(expected=JoinColumnMismatchException.class)
    public void groupAddNoReferencedSingleColumnToMultiColumn() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).colBigInt("id2", false).pk("id","id2");
        builder.table(A_NAME).colBigInt("id", false).colBigInt("other_id").colBigInt("other_id2").pk("id");
        parseAndRun("ALTER GROUP ADD TABLE a(other_id) TO c");
    }

    @Test(expected=SQLParserException.class)
    public void groupAddReferencedListCannotBeEmpty() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER GROUP ADD TABLE a(other_id) TO c()");
    }


    //
    // ALTER GROUP DROP
    //

    @Test
    public void groupDropTableTwoTableGroup() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).pk("id");
        builder.table(A_NAME).colBigInt("id", false).colBigInt("cid").pk("id").joinTo(C_NAME).on("cid", "id");
        parseAndRun("ALTER GROUP DROP TABLE a");
        expectGroupIsSame(C_NAME, A_NAME, false);
    }

    @Test
    public void groupDropTableLeafOfMultiple() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER GROUP DROP TABLE i");
        expectGroupIsSame(C_NAME, I_NAME, false);
    }

    //
    // ALTER ADD FOREIGN KEY
    // 
    
    @Test
    public void fkAddSimple () throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).pk("id");
        builder.table(A_NAME).colBigInt("id", false).colBigInt("cid").pk("id");
        parseAndRun("ALTER TABLE a ADD FOREIGN KEY (cid) REFERENCES c (id)");
        
        Table a = ddlFunctions.ais.getTable(A_NAME);
        assertEquals(a.getForeignKeys().size(), 1);
        assertEquals(a.getReferencingForeignKeys().size(), 1);
        assertEquals(a.getReferencedForeignKeys().size(), 0);
    }
    
    @Test
    public void fkAddNamed() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).pk("id");
        builder.table(A_NAME).colBigInt("id", false).colBigInt("cid").pk("id");
        parseAndRun("ALTER TABLE a ADD CONSTRAINT test_constraint FOREIGN KEY (cid) REFERENCES c (id)");
        Table a = ddlFunctions.ais.getTable(A_NAME);
        assertEquals(a.getForeignKeys().size(), 1);
        assertEquals(a.getReferencingForeignKeys().size(), 1);
        assertEquals(a.getReferencedForeignKeys().size(), 0);
        assertNotNull(a.getReferencingForeignKey("test_constraint"));
    }

    @Test
    public void fkNonDefaultSchema() throws StandardException {
        String otherSchema = SCHEMA + "2";
        builder.table(otherSchema, "p").colBigInt("pid", false).pk("pid");
        builder.table(otherSchema, "c").colBigInt("cid", false).colBigInt("pid", true).pk("cid");
        parseAndRun(String.format("ALTER TABLE %s.%s ADD FOREIGN KEY(pid) REFERENCES %s.%s(pid)", otherSchema, "c", otherSchema, "p"));
        Table c = ddlFunctions.ais.getTable(otherSchema, "c");
        assertEquals(c.getForeignKeys().size(), 1);
        assertEquals(c.getReferencingForeignKeys().size(), 1);
        assertEquals(c.getReferencedForeignKeys().size(), 0);
    }

    //
    // DROP FOREIGN KEY
    //

    @Test
    public void fkDropSimple() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).pk("id");
        builder.table(A_NAME).colBigInt("id", false).colBigInt("cid").pk("id");
        parseAndRun("ALTER TABLE a ADD FOREIGN KEY (cid) REFERENCES c (id)");

        Table a = ddlFunctions.ais.getTable(A_NAME);
        assertEquals(a.getForeignKeys().size(), 1);
        
        parseAndRun ("ALTER TABLE a DROP FOREIGN KEY");
        a = ddlFunctions.ais.getTable(A_NAME);
        assertEquals(a.getForeignKeys().size(), 0);
    }

    @Test
    public void fkDropByName() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).pk("id");
        builder.table(A_NAME).colBigInt("id", false).colBigInt("cid").pk("id");
        parseAndRun("ALTER TABLE a ADD CONSTRAINT cid FOREIGN KEY (cid) REFERENCES c (id)");

        Table a = ddlFunctions.ais.getTable(A_NAME);
        assertEquals(a.getForeignKeys().size(), 1);
        assertNotNull(a.getReferencingForeignKey("cid"));
        
        parseAndRun ("ALTER TABLE a DROP FOREIGN KEY cid");
        a = ddlFunctions.ais.getTable(A_NAME);
        assertEquals(a.getForeignKeys().size(), 0);
        assertNull(a.getReferencingForeignKey("cid"));
    }
    
    @Test
    public void fkDropByConstraintName() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).pk("id");
        builder.table(A_NAME).colBigInt("id", false).colBigInt("cid").pk("id");
        parseAndRun("ALTER TABLE a ADD CONSTRAINT cid FOREIGN KEY (cid) REFERENCES c (id)");

        Table a = ddlFunctions.ais.getTable(A_NAME);
        assertEquals(a.getForeignKeys().size(), 1);
        assertNotNull(a.getReferencingForeignKey("cid"));
        
        parseAndRun ("ALTER TABLE a DROP CONSTRAINT cid");
        a = ddlFunctions.ais.getTable(A_NAME);
        assertEquals(a.getForeignKeys().size(), 0);
        assertNull(a.getReferencingForeignKey("cid"));
    }

    @Test
    public void fkDropByNameWithUnique() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).pk("id");
        builder.table(A_NAME).colBigInt("id", false).colBigInt("cid").pk("id").uniqueConstraint("cid_unique",
                                                                                                "cid_unique",
                                                                                                "cid");
        parseAndRun("ALTER TABLE a ADD CONSTRAINT fk_cid FOREIGN KEY (cid) REFERENCES c (id)");
        Table a = ddlFunctions.ais.getTable(A_NAME);
        assertEquals(a.getForeignKeys().size(), 1);
        assertNotNull(a.getReferencingForeignKey("fk_cid"));
        parseAndRun ("ALTER TABLE a DROP CONSTRAINT fk_cid");
        a = ddlFunctions.ais.getTable(A_NAME);
        assertEquals(a.getForeignKeys().size(), 0);
        assertNull(a.getReferencingForeignKey("fk_cid"));
        expectFinalTable(A_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL",  "cid MCOMPAT_ BIGINT(21) NOT NULL", "UNIQUE cid_unique(cid)", "PRIMARY(id)");
    }

    @Test
    public void fkDropUniqueNotFK() throws StandardException {
        builder.table(C_NAME).colBigInt("id", false).pk("id");
        builder.table(A_NAME).colBigInt("id", false).colBigInt("cid").pk("id").uniqueConstraint("cid_unique",
                                                                                                "cid_unique",
                                                                                                "cid");
        parseAndRun("ALTER TABLE a ADD CONSTRAINT fk_cid FOREIGN KEY (cid) REFERENCES c (id)");
        Table a = ddlFunctions.ais.getTable(A_NAME);
        assertEquals(a.getForeignKeys().size(), 1);
        assertNotNull(a.getReferencingForeignKey("fk_cid"));
        parseAndRun ("ALTER TABLE a DROP CONSTRAINT cid_unique");
        a = ddlFunctions.ais.getTable(A_NAME);
        expectFinalTable(A_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL",  "cid MCOMPAT_ BIGINT(21) NOT NULL", "fk_cid(cid)", "PRIMARY(id)");
    }

    //
    // add Index
    //

    @Test
    public void addIndex() throws StandardException {
        builder.table(C_NAME).colBigInt("id");
        parseAndRun("ALTER TABLE c ADD INDEX idindex(id)");
        Table c = ddlFunctions.ais.getTable(C_NAME);
        assertEquals(c.getIndex("idindex").getNameString(), "test.c.idindex");
        expectFinalTable(C_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL, idindex(id)");
    }

    @Test
    public void dropIndex() throws StandardException {
        builder.table(C_NAME).colBigInt("id").key("idindex", "id").colBigInt("id2").key("idindex2", "id2");
        parseAndRun("ALTER TABLE c DROP INDEX idindex");
        Table c = ddlFunctions.ais.getTable(C_NAME);
        assertEquals(c.getIndex("idindex2").getNameString(), "test.c.idindex2");
        assertNull(c.getIndex("idindex"));
        expectFinalTable(C_NAME, "id MCOMPAT_ BIGINT(21) NOT NULL, id2 MCOMPAT_ BIGINT(21) NOT NULL, idindex2(id2)");
    }

    //
    // IF EXISTS
    //

    @Test
    public void alterIfExists() throws StandardException {
        assertEquals( null, parseAndRun("ALTER TABLE IF EXISTS c ADD COLUMN x INT") );
    }

    @Test
    public void alterDropColumnIfExists() throws StandardException {
        builder.table(C_NAME).colBigInt("id");
        assertEquals( ChangeLevel.NONE, parseAndRun("ALTER TABLE c DROP COLUMN IF EXISTS x") );
    }

    @Test
    public void alterDropConstraintIfExists() throws StandardException {
        builder.table(C_NAME).colBigInt("id");
        parseAndRun("ALTER TABLE c DROP CONSTRAINT IF EXISTS x");
    }

    @Test
    public void alterDropUniqueIfExists() throws StandardException {
        builder.table(C_NAME).colBigInt("id");
        assertEquals( ChangeLevel.NONE, parseAndRun("ALTER TABLE c DROP UNIQUE IF EXISTS x") );
    }

    @Test
    public void alterDropPrimaryIfExists() throws StandardException {
        builder.table(C_NAME).colBigInt("id");
        assertEquals( ChangeLevel.NONE, parseAndRun("ALTER TABLE c DROP PRIMARY KEY IF EXISTS") );
    }

    @Test
    public void alterDropForeignKeyIfExists() throws StandardException {
        builder.table(C_NAME).colBigInt("id");
        assertEquals( ChangeLevel.NONE, parseAndRun("ALTER TABLE c DROP FOREIGN KEY IF EXISTS x") );
    }

    @Test
    public void alterDropGroupingForeignKeyIfExists() throws StandardException {
        builder.table(C_NAME).colBigInt("id");
        assertEquals( ChangeLevel.NONE, parseAndRun("ALTER TABLE c DROP GROUPING FOREIGN KEY IF EXISTS x") );
    }

    @Test
    public void alterDropIndexIfExists() throws StandardException {
        builder.table(C_NAME).colBigInt("id");
        assertEquals( ChangeLevel.NONE, parseAndRun("ALTER TABLE c DROP INDEX IF EXISTS x") );
    }


    //
    // Test helpers
    //

    private ChangeLevel parseAndRun(String sqlText) throws StandardException {
        StatementNode node = parser.parseStatement(sqlText);
        assertEquals("Was alter", AlterTableNode.class, node.getClass());
        ddlFunctions = new DDLFunctionsMock(builder.ais());
        return AlterTableDDL.alterTable(ddlFunctions, null, null, SCHEMA, (AlterTableNode)node, null);
    }

    private void expectGroupIsSame(TableName t1, TableName t2, boolean equal) {
        // Only check the name of the group, DDLFunctionsMock doesn't re-serialize
        Table table1 = ddlFunctions.ais.getTable(t1);
        Table table2 = ddlFunctions.ais.getTable(t2);
        String groupName1 = ((table1 != null) && (table1.getGroup() != null)) ? table1.getGroup().getName().toString() : "<NO_GROUP>1";
        String groupName2 = ((table2 != null) && (table2.getGroup() != null)) ? table2.getGroup().getName().toString() : "<NO_GROUP>2";
        if(equal) {
            assertEquals("Same group for tables " + t1 + "," + t2, groupName1, groupName2);
        } else if(groupName1.equals(groupName2)) {
            fail("Expected different group for tables " + t1 + "," + t2);
        }
    }

    private void expectChildOf(TableName pTableName, TableName cTableName) {
        // Only check the names of tables, DDLFunctionsMock doesn't re-serialize
        Table table1 = ddlFunctions.ais.getTable(cTableName);
        Table parent = (table1.getParentJoin() != null) ? table1.getParentJoin().getParent() : null;
        TableName parentName = (parent != null) ? parent.getName() : null;
        assertEquals(cTableName + " parent name", pTableName, parentName);
    }

    private void expectColumnChanges(String... changes) {
        assertEquals("Column changes", Arrays.asList(changes).toString(), ddlFunctions.columnChangeDesc.toString());
    }

    private void expectIndexChanges(String... changes) {
        assertEquals("Index changes", Arrays.asList(changes).toString(), ddlFunctions.indexChangeDesc.toString());
    }

    private void expectFinalTable(TableName table, String... parts) {
        String expected = table.toString() + "(" + Strings.join(Arrays.asList(parts), ", ") + ")";
        assertEquals("Final structure for " + table, expected, ddlFunctions.newTableDesc);
    }

    private void expectUnchangedTables(TableName... names) {
        for(TableName name : names) {
            String expected = name.toString();
            if(name == C_NAME) {
                expected += "(id MCOMPAT_ BIGINT(21) NOT NULL, c_c MCOMPAT_ BIGINT(21) NULL, PRIMARY(id))";
            } else if(name == O_NAME) {
                expected += "(id MCOMPAT_ BIGINT(21) NOT NULL, cid MCOMPAT_ BIGINT(21) NULL, o_o MCOMPAT_ BIGINT(21) NULL, fk1(cid), PRIMARY(id), join(cid->id))";
            } else if(name == I_NAME) {
                expected += "(id MCOMPAT_ BIGINT(21) NOT NULL, oid MCOMPAT_ BIGINT(21) NULL, i_i MCOMPAT_ BIGINT(21) NULL, fk2(oid), PRIMARY(id), join(oid->id))";
            } else if(name == A_NAME) {
                expected += "(id MCOMPAT_ BIGINT(21) NOT NULL, other_id MCOMPAT_ BIGINT(21) NULL, PRIMARY(id))";
            } else {
                fail("Unknown table: " + name);
            }
            Table table = ddlFunctions.ais.getTable(name);
            String actual = simpleDescribeTable(table);
            assertEquals(name + " was unchanged", expected, actual);
        }
    }

    private void buildCOIJoinedAUnJoined() {
        builder.table(C_NAME).colBigInt("id", false).colBigInt("c_c", true).pk("id");
        builder.table(O_NAME).colBigInt("id", false).colBigInt("cid", true).colBigInt("o_o", true).pk("id").joinTo(SCHEMA, "c", "fk1").on("cid", "id");
        builder.table(I_NAME).colBigInt("id", false).colBigInt("oid", true).colBigInt("i_i", true).pk("id").joinTo(SCHEMA, "o", "fk2").on("oid", "id");
        builder.table(A_NAME).colBigInt("id", false).colBigInt("other_id", true).pk("id");
    }

    private void buildCWithGeneratedID(int startWith, boolean always) {
        builder.table(C_NAME).autoIncInt("id", startWith, always).pk("id");
    }

    private void buildCWithID() {
        builder.table(C_NAME).colInt("id", false).pk("id");
    }

    private static class DDLFunctionsMock extends DDLFunctionsMockBase {
        final AkibanInformationSchema ais;
        final List<String> columnChangeDesc = new ArrayList<>();
        final List<String> indexChangeDesc = new ArrayList<>();
        String newTableDesc = "";
        String newSeqDesc = "";

        public DDLFunctionsMock(AkibanInformationSchema ais) {
            this.ais = ais;
        }

        @Override
        public ChangeLevel alterTable(Session session, TableName tableName, Table newDefinition,
                                      List<TableChange> columnChanges, List<TableChange> indexChanges, QueryContext context) {
            if(ais.getTable(tableName) == null) {
                throw new NoSuchTableException(tableName);
            }
            ais.getTables().remove(tableName);
            ais.getTables().put(newDefinition.getName(), newDefinition);
            for(TableChange change : columnChanges) {
                columnChangeDesc.add(change.toString());
            }
            for(TableChange change : indexChanges) {
                indexChangeDesc.add(change.toString());
            }
            newTableDesc = simpleDescribeTable(newDefinition);
            return ChangeLevel.NONE; // Doesn't matter, just can't be null
        }

        @Override
        public void alterSequence(Session session, TableName sequenceName, Sequence newDefinition) {
            if(ais.getSequence(sequenceName) == null) {
                throw new NoSuchSequenceException(sequenceName);
            }
            assert sequenceName.equals(newDefinition.getSequenceName());
            ais.getSequences().remove(sequenceName);
            ais.getSequences().put(newDefinition.getSequenceName(), newDefinition);
            newSeqDesc = simpleDescribeSequence(newDefinition);
        }

        @Override
        public AkibanInformationSchema getAIS(Session session) {
            return ais;
        }
    }

    private static TableName tn(String schema, String table) {
        return new TableName(schema, table);
    }

    private static String simpleDescribeTable(Table table) {
        // Trivial description: ordered columns and indexes
        StringBuilder sb = new StringBuilder();
        sb.append(table.getName()).append('(');
        boolean first = true;
        for(Column col : table.getColumns()) {
            sb.append(first ? "" : ", ").append(col.getName()).append(' ');
            first = false;
            sb.append(col.getType().toString());
            String defaultVal = col.getDefaultValue();
            if(defaultVal != null) {
                sb.append(" DEFAULT ");
                sb.append(defaultVal);
            }
            Boolean identity = col.getDefaultIdentity();
            if(identity != null) {
                Sequence seq = col.getIdentityGenerator();
                sb.append(" GENERATED ");
                sb.append(identity ? "BY DEFAULT" : "ALWAYS");
                sb.append(" AS IDENTITY (START WITH ");
                sb.append(seq.getStartsWith());
                sb.append(", INCREMENT BY ");
                sb.append(seq.getIncrement());
                sb.append(')');
            }
        }
        List<TableIndex> sortedIndexes = new ArrayList<>(table.getIndexes());
        Collections.sort(sortedIndexes, new Comparator<TableIndex>() {
            @Override
            public int compare(TableIndex x, TableIndex y) {
                return String.CASE_INSENSITIVE_ORDER.compare(x.getIndexName().getName(), y.getIndexName().getName());
            }
        });
        for(Index index : sortedIndexes) {
            sb.append(", ");
            if(!index.isPrimaryKey() && index.isUnique()) {
                sb.append("UNIQUE ");
            }
            sb.append(index.getIndexName().getName()).append('(');
            first = true;
            for(IndexColumn indexColumn : index.getKeyColumns()) {
                sb.append(first ? "" : ',').append(indexColumn.getColumn().getName());
                first = false;
            }
            sb.append(')');
        }
        Join join = table.getParentJoin();
        if(join != null) {
            sb.append(", join(");
            first = true;
            for(JoinColumn joinColumn : join.getJoinColumns()) {
                sb.append(first ? "" : ", ").append(joinColumn.getChild().getName()).append("->").append(joinColumn.getParent().getName());
                first = false;
            }
            sb.append(")");
        }
        sb.append(')');
        return sb.toString();
    }

    private static String simpleDescribeSequence(Sequence s) {
        return String.format("Sequence(%s,%d,%d,%d,%d,%b)",
                             s.getSequenceName(),
                             s.getStartsWith(),
                             s.getIncrement(),
                             s.getMinValue(),
                             s.getMaxValue(),
                             s.isCycle());
    }
}
