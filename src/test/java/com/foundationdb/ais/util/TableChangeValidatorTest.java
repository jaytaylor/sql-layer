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

package com.foundationdb.ais.util;

import com.foundationdb.ais.model.*;
import com.foundationdb.ais.model.ForeignKey.Action;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.ais.model.aisb2.NewTableBuilder;
import com.foundationdb.ais.util.TableChange.ChangeType;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.foundationdb.ais.util.ChangedTableDescription.ParentChange;
import static com.foundationdb.ais.util.TableChangeValidator.ChangeLevel;
import static com.foundationdb.ais.util.TableChangeValidatorException.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TableChangeValidatorTest {
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";
    private static final TableName TABLE_NAME = new TableName(SCHEMA, TABLE);

    private static final String EMPTY_LIST_STR = Collections.emptyList().toString();
    private static final String EMPTY_MAP_STR = Collections.emptyMap().toString();

    private static final String[] NO_INDEX_CHANGE = { EMPTY_LIST_STR, EMPTY_MAP_STR, EMPTY_MAP_STR };
    private static final String NO_IDENTITY_CHANGE = "";

    private final List<TableChange> NO_CHANGES = Collections.emptyList();
    private final List<TableChange> AUTO_CHANGES = new ArrayList<>();

    private final TypesTranslator typesTranslator = MTypesTranslator.INSTANCE;

    private NewTableBuilder builder(TableName name) {
        return AISBBasedBuilder.create(SCHEMA, typesTranslator).table(name);
    }

    private Table table(NewAISBuilder builder) {
        AkibanInformationSchema ais = builder.ais();
        assertEquals("User table count", 1, ais.getTables().size());
        return ais.getTables().values().iterator().next();
    }

    private Table table(NewAISBuilder builder, TableName tableName) {
        Table table = builder.ais().getTable(tableName);
        assertNotNull("Found table: " + tableName, table);
        return table;
    }

    private static TableChangeValidator validate(Table t1, Table t2,
                                                 List<TableChange> columnChanges, List<TableChange> indexChanges,
                                                 ChangeLevel expectedChangeLevel) {
        return validate(t1, t2, columnChanges, indexChanges, expectedChangeLevel,
                        asList(changeDesc(TABLE_NAME, TABLE_NAME, false, ParentChange.NONE, "PRIMARY", "PRIMARY")),
                        false, false, NO_INDEX_CHANGE, NO_IDENTITY_CHANGE);
    }

    private static TableChangeValidator validate(Table t1, Table t2,
                                                 List<TableChange> columnChanges, List<TableChange> indexChanges,
                                                 ChangeLevel expectedChangeLevel,
                                                 List<String> expectedChangedTables) {
        return validate(t1, t2, columnChanges, indexChanges, expectedChangeLevel,
                        expectedChangedTables,
                        false, false, NO_INDEX_CHANGE, NO_IDENTITY_CHANGE);
    }

    private static TableChangeValidator validate(Table t1, Table t2,
                                                 List<TableChange> columnChanges, List<TableChange> indexChanges,
                                                 ChangeLevel expectedChangeLevel,
                                                 List<String> expectedChangedTables,
                                                 boolean expectedParentChange,
                                                 boolean expectedPrimaryKeyChange,
                                                 String[] expectedGIChanges,
                                                 String expectedIdentityChange) {
        TableChangeValidator validator = new TableChangeValidator(t1, t2, columnChanges, indexChanges);
        validator.compare();
        assertEquals("Final change level", expectedChangeLevel, validator.getFinalChangeLevel());
        assertEquals("Parent changed", expectedParentChange, validator.isParentChanged());
        assertEquals("Primary key changed", expectedPrimaryKeyChange, validator.isPrimaryKeyChanged());
        assertEquals("Changed tables", expectedChangedTables.toString(), validator.getState().descriptions.toString());
        Collections.sort(validator.getState().droppedGI);
        assertEquals("Dropped group index", expectedGIChanges[0], validator.getState().droppedGI.toString());
        assertEquals("Recreate group index", expectedGIChanges[1], validator.getState().affectedGI.toString());
        assertEquals("Rebuild group index", expectedGIChanges[2], validator.getState().dataAffectedGI.toString());
        assertEquals("Unmodified changes", "[]", validator.getUnmodifiedChanges().toString());
        assertEquals("Changed identity", expectedIdentityChange, identityChangeDesc(validator.getState().descriptions));
        return validator;
    }

    private static Map<String,String> map(String... pairs) {
        assertTrue("Even number of pairs", (pairs.length % 2) == 0);
        Map<String,String> map = new TreeMap<>();
        for(int i = 0; i < pairs.length; i += 2) {
            map.put(pairs[i], pairs[i+1]);
        }
        return map;
    }

    private static String changeDesc(TableName oldName, TableName newName, boolean newGroup, ParentChange parentChange, String... preservedIndexPairs) {
        return ChangedTableDescription.toString(oldName, newName, newGroup, parentChange, map(preservedIndexPairs));
    }

    private static String identityChangeDesc(Collection<ChangedTableDescription> tableChanges) {
        StringBuilder str = new StringBuilder();
        for (ChangedTableDescription change : tableChanges) {
            if (!change.getDroppedSequences().isEmpty()) {
                str.append("-").append(change.getDroppedSequences());
            }
            if (!change.getIdentityAdded().isEmpty()) {
                str.append("+").append(change.getIdentityAdded());
            }
        }
        return str.toString();
    }
    
    //
    // Table
    //

    @Test
    public void sameTable() {
        Table t = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        validate(t, t, NO_CHANGES, NO_CHANGES, ChangeLevel.NONE);
    }

    @Test
    public void unchangedTable() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        validate(t1, t2, NO_CHANGES, NO_CHANGES, ChangeLevel.NONE);
    }

    @Test
    public void changeOnlyTableName() {
        TableName name2 = new TableName("x", "y");
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        Table t2 = table(builder(name2).colBigInt("id").pk("id"));
        validate(t1, t2, NO_CHANGES, NO_CHANGES, ChangeLevel.METADATA,
                 asList(changeDesc(TABLE_NAME, name2, false, ParentChange.NONE, "PRIMARY", "PRIMARY")));
    }

    @Test
    public void changeDefaultCharset() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        t1.setCharsetAndCollation("utf8", "ucs_binary");
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        t1.setCharsetAndCollation("utf16", "ucs_binary");
        validate(t1, t2, NO_CHANGES, NO_CHANGES, ChangeLevel.METADATA);
    }

    @Test
    public void changeDefaultCollation() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        t1.setCharsetAndCollation("utf8", "ucs_binary");
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        t1.setCharsetAndCollation("utf8", "en_us_ci");
        validate(t1, t2, NO_CHANGES, NO_CHANGES, ChangeLevel.METADATA);
    }

    //
    // Column
    //

    @Test
    public void addColumn() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        validate(t1, t2, asList(TableChange.createAdd("x")), NO_CHANGES, ChangeLevel.TABLE);
    }

    @Test
    public void addIdentityColumn() {
        final TableName SEQ_NAME = new TableName(SCHEMA, "seq-1");
        Table t1 = table(builder(TABLE_NAME).colBigInt("x"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("x").colBigInt("id").pk("id").sequence(SEQ_NAME.getTableName()));
        t2.getColumn("id").setIdentityGenerator(t2.getAIS().getSequence(SEQ_NAME));
        t2.getColumn("id").setDefaultIdentity(true);
        validate(t1, t2,
                 asList(TableChange.createDrop(Column.AKIBAN_PK_NAME),TableChange.createAdd("id")),
                 asList(TableChange.createAdd("PRIMARY")),
                 ChangeLevel.GROUP,
                 asList(changeDesc(TABLE_NAME, TABLE_NAME, false, ParentChange.NONE)),
                 false, true, NO_INDEX_CHANGE,
                 "-[test.t]+[id]");
    }

    @Test
    public void dropColumn() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        validate(t1, t2, asList(TableChange.createDrop("x")), NO_CHANGES, ChangeLevel.TABLE);
    }

    @Test
    public void modifyColumnDataType() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("y").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colString("y", 32).pk("id"));
        validate(t1, t2, asList(TableChange.createModify("y", "y")), NO_CHANGES, ChangeLevel.TABLE);
    }

    @Test
    public void modifyColumnName() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("y").pk("id"));
        validate(t1, t2, asList(TableChange.createModify("x", "y")), NO_CHANGES, ChangeLevel.METADATA);
    }

    @Test
    public void modifyColumnNotNullToNull() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x", false).pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x", true).pk("id"));
        validate(t1, t2, asList(TableChange.createModify("x", "x")), NO_CHANGES, ChangeLevel.METADATA);
    }

    @Test
    public void modifyColumnNullToNotNull() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x", true).pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x", false).pk("id"));
        validate(t1, t2, asList(TableChange.createModify("x", "x")), NO_CHANGES, ChangeLevel.METADATA_CONSTRAINT);
    }

    @Test
    public void modifyAddGeneratedBy() {
        final TableName SEQ_NAME = new TableName(SCHEMA, "seq-1");
        Table t1 = table(builder(TABLE_NAME).colInt("id", false).pk("id"));
        Table t2 = table(builder(TABLE_NAME).colInt("id", false).pk("id").sequence(SEQ_NAME.getTableName()));
        t2.getColumn("id").setIdentityGenerator(t2.getAIS().getSequence(SEQ_NAME));
        t2.getColumn("id").setDefaultIdentity(true);
        validate(t1, t2, asList(TableChange.createModify("id", "id")), NO_CHANGES, ChangeLevel.METADATA,
                 asList(changeDesc(TABLE_NAME, TABLE_NAME, false, ParentChange.NONE, "PRIMARY", "PRIMARY")),
                 false, false, NO_INDEX_CHANGE, "+[id]");
    }

    @Test
    public void modifyDropGeneratedBy() {
        final TableName SEQ_NAME = new TableName(SCHEMA, "seq-1");
        Table t1 = table(builder(TABLE_NAME).colInt("id", false).pk("id").sequence(SEQ_NAME.getTableName()));
        t1.getColumn("id").setIdentityGenerator(t1.getAIS().getSequence(SEQ_NAME));
        t1.getColumn("id").setDefaultIdentity(true);
        Table t2 = table(builder(TABLE_NAME).colInt("id", false).pk("id"));
        validate(t1, t2, asList(TableChange.createModify("id", "id")), NO_CHANGES, ChangeLevel.METADATA,
                 asList(changeDesc(TABLE_NAME, TABLE_NAME, false, ParentChange.NONE, "PRIMARY", "PRIMARY")),
                 false, false, NO_INDEX_CHANGE, "-[test.seq-1]");
    }

    @Test
    public void modifyColumnAddDefault() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x", false).colInt("c1", true).pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x", false).colInt("c1", true).pk("id"));
        t2.getColumn("c1").setDefaultValue("42");
        validate(t1, t2, asList(TableChange.createModify("c1", "c1")), NO_CHANGES, ChangeLevel.METADATA);
    }

    @Test
    public void modifyColumnDropDefault() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x", false).colInt("c1", true).pk("id"));
        t1.getColumn("c1").setDefaultValue("42");
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x", false).colInt("c1", true).pk("id"));
        validate(t1, t2, asList(TableChange.createModify("c1", "c1")), NO_CHANGES, ChangeLevel.METADATA);
    }

    //
    // Column (negative)
    //

    @Test(expected=UndeclaredColumnChangeException.class)
    public void addColumnUnspecified() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        validate(t1, t2, NO_CHANGES, NO_CHANGES, null);
    }

    @Test(expected=UnchangedColumnNotPresentException.class)
    public void dropColumnUnspecified() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        validate(t1, t2, NO_CHANGES, NO_CHANGES, null);
    }

    @Test(expected=DropColumnNotPresentException.class)
    public void dropColumnUnknown() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        validate(t1, t2, asList(TableChange.createDrop("x")), NO_CHANGES, null);
    }

    @Test
    public void modifyColumnNotChanged() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        TableChangeValidator tcv = new TableChangeValidator(t1, t2, asList(TableChange.createModify("x", "x")), NO_CHANGES);
        tcv.compare();
        assertEquals("Final change level", ChangeLevel.NONE, tcv.getFinalChangeLevel());
        assertEquals("Unmodified change count", 1, tcv.getUnmodifiedChanges().size());
    }

    @Test(expected=ModifyColumnNotPresentException.class)
    public void modifyColumnUnknown() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        validate(t1, t2, asList(TableChange.createModify("y", "y")), NO_CHANGES, null);
    }

    @Test(expected=UndeclaredColumnChangeException.class)
    public void modifyColumnUnspecified() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colString("x", 32).pk("id"));
        validate(t1, t2, NO_CHANGES, NO_CHANGES, null);
    }

    //
    // Index
    //

    @Test
    public void addIndex() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        validate(t1, t2, NO_CHANGES, asList(TableChange.createAdd("x")), ChangeLevel.INDEX);
    }

    @Test
    public void dropIndex() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        validate(t1, t2, NO_CHANGES, asList(TableChange.createDrop("x")), ChangeLevel.INDEX);
    }

    @Test
    public void modifyIndexedColumn() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").colBigInt("y").key("k", "x").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").colBigInt("y").key("k", "y").pk("id"));
        validate(t1, t2, NO_CHANGES, asList(TableChange.createModify("k", "k")), ChangeLevel.INDEX);
    }

    @Test
    public void modifyIndexedType() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colString("x", 32).key("x", "x").pk("id"));
        validate(t1, t2, asList(TableChange.createModify("x", "x")), asList(TableChange.createModify("x", "x")),
                 ChangeLevel.TABLE);
    }

    @Test
    public void modifyIndexName() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("a", "x").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("b", "x").pk("id"));
        validate(t1, t2, NO_CHANGES, asList(TableChange.createModify("a", "b")), ChangeLevel.METADATA);
    }

    //
    // Index (negative)
    //

    @Test
    public void addIndexUnspecified() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        validate(t1, t2, NO_CHANGES, AUTO_CHANGES, ChangeLevel.INDEX);
        assertEquals("index changes", 1, AUTO_CHANGES.size());
        assertEquals("change type", ChangeType.ADD, AUTO_CHANGES.get(0).getChangeType());
    }

    @Test
    public void dropIndexUnspecified() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        validate(t1, t2, NO_CHANGES, AUTO_CHANGES, ChangeLevel.INDEX);
        assertEquals("index changes", 1, AUTO_CHANGES.size());
    }

    @Test(expected=DropIndexNotPresentException.class)
    public void dropIndexUnknown() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        validate(t1, t2, NO_CHANGES, asList(TableChange.createDrop("x")), null);
    }

    @Test
    public void modifyIndexNotChanged() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        TableChangeValidator tcv = new TableChangeValidator(t1, t2, NO_CHANGES, asList(
                TableChange.createModify("x", "x")));
        tcv.compare();
        assertEquals("Final change level", ChangeLevel.NONE, tcv.getFinalChangeLevel());
        assertEquals("Unmodified change count", 1, tcv.getUnmodifiedChanges().size());
    }

    @Test(expected=ModifyIndexNotPresentException.class)
    public void modifyIndexUnknown() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        validate(t1, t2, NO_CHANGES, asList(TableChange.createModify("y", "y")), null);
    }

    @Test
    public void modifyIndexUnspecified() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").colBigInt("y").key("k", "x").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").colBigInt("y").key("k", "y").pk("id"));
        validate(t1, t2, NO_CHANGES, AUTO_CHANGES, ChangeLevel.INDEX);
        assertEquals("index changes", 1, AUTO_CHANGES.size());
    }

    @Test
    public void modifyIndexedColumnIndexUnspecified() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colString("x", 32).key("x", "x").pk("id"));
        validate(t1, t2, asList(TableChange.createModify("x", "x")), AUTO_CHANGES, ChangeLevel.TABLE);
        assertEquals("index changes", 1, AUTO_CHANGES.size());
    }

    //
    // Group
    //

    @Test
    public void modifyPKColumnTypeSingleTableGroup() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colString("id", 32).pk("id"));
        validate(t1, t2,
                 asList(TableChange.createModify("id", "id")),
                 asList(TableChange.createModify(Index.PRIMARY_KEY_CONSTRAINT, Index.PRIMARY_KEY_CONSTRAINT)),
                 ChangeLevel.GROUP,
                 asList(changeDesc(TABLE_NAME, TABLE_NAME, false, ParentChange.NONE)),
                 false, true, NO_INDEX_CHANGE, NO_IDENTITY_CHANGE);
    }

    @Test
    public void dropPrimaryKeySingleTableGroup() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id"));
        validate(t1, t2,
                asList(TableChange.createAdd(Column.AKIBAN_PK_NAME)),
                 asList(TableChange.createDrop(Index.PRIMARY_KEY_CONSTRAINT)),
                 ChangeLevel.GROUP,
                 asList(changeDesc(TABLE_NAME, TABLE_NAME, false, ParentChange.NONE)),
                 false, true, NO_INDEX_CHANGE, "+[__akiban_pk]");
    }

    @Test
    public void dropPrimaryKeyColumn() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id").colString("name", 32));
        Table t2 = table(builder(TABLE_NAME).colString("name", 32));
        validate(t1, t2,
                asList(TableChange.createDrop("id"),TableChange.createAdd(Column.AKIBAN_PK_NAME)),
                asList(TableChange.createDrop(Index.PRIMARY_KEY_CONSTRAINT)),
                ChangeLevel.GROUP,
                asList(changeDesc(TABLE_NAME, TABLE_NAME, false, ParentChange.NONE)),
                false, true,
                NO_INDEX_CHANGE,
                "+[__akiban_pk]"
                );
    }
    
    @Test 
    public void dropPrimaryKeyIdentityColumn() {
        Table t1 = table(builder(TABLE_NAME).autoIncInt("id",1).pk("id").colString("name", 32));
        Table t2 = table(builder(TABLE_NAME).colString("name", 32));
        validate(t1, t2,
                asList(TableChange.createDrop("id"),TableChange.createAdd(Column.AKIBAN_PK_NAME)),
                asList(TableChange.createDrop(Index.PRIMARY_KEY_CONSTRAINT)),
                ChangeLevel.GROUP,
                asList(changeDesc(TABLE_NAME, TABLE_NAME, false, ParentChange.NONE)),
                false, true,
                NO_INDEX_CHANGE,
                "-[test.temp-seq-t-id]+[__akiban_pk]"
                );
    }

    @Test
    public void dropParentJoinTwoTableGroup() {
        TableName parentName = new TableName(SCHEMA, "parent");
        Table t1 = table(
                builder(parentName).colInt("id").pk("id").
                        table(TABLE_NAME).colBigInt("id").colInt("pid").pk("id").joinTo(SCHEMA, "parent", "fk").on("pid", "id"),
                TABLE_NAME
        );
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colInt("pid").pk("id"));
        validate(t1, t2,
                 NO_CHANGES,
                 asList(TableChange.createDrop("__akiban_fk")),
                 ChangeLevel.GROUP,
                 asList(changeDesc(TABLE_NAME, TABLE_NAME, true, ParentChange.DROP)),
                 true, false, NO_INDEX_CHANGE, NO_IDENTITY_CHANGE);
    }

    @Test
    public void dropPrimaryKeyMiddleOfGroup() {
        TableName cName = new TableName(SCHEMA, "c");
        TableName oName = new TableName(SCHEMA, "o");
        TableName iName = new TableName(SCHEMA, "i");
        NewAISBuilder builder1 = AISBBasedBuilder.create(typesTranslator);
        builder1.table(cName).colBigInt("id", false).pk("id")
                .table(oName).colBigInt("id", false).colBigInt("cid", true).pk("id").joinTo(SCHEMA, "c", "fk1").on("cid", "id")
                .table(iName).colBigInt("id", false).colBigInt("oid", true).pk("id").joinTo(SCHEMA, "o", "fk2").on("oid", "id");
        NewAISBuilder builder2 = AISBBasedBuilder.create(typesTranslator);
        builder2.table(cName).colBigInt("id", false).pk("id")
                .table(oName).colBigInt("id", false).colBigInt("cid", true).joinTo(SCHEMA, "c", "fk1").on("cid", "id")
                .table(iName).colBigInt("id", false).colBigInt("oid", true).pk("id").joinTo(SCHEMA, "o", "fk2").on("oid", "id");
        Table t1 = builder1.unvalidatedAIS().getTable(oName);
        Table t2 = builder2.unvalidatedAIS().getTable(oName);
        validate(
                t1, t2,
                asList(TableChange.createAdd(Column.AKIBAN_PK_NAME)),
                asList(TableChange.createDrop(Index.PRIMARY_KEY_CONSTRAINT)),
                ChangeLevel.GROUP,
                asList(
                        changeDesc(oName, oName, false, ParentChange.NONE),
                        changeDesc(iName, iName, true, ParentChange.DROP)
                ),
                false,
                true,
                NO_INDEX_CHANGE,
                "+[__akiban_pk]"
        );
    }

    //
    // Multi-part
    //

    @Test
    public void addAndDropColumn() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("y").pk("id"));
        validate(t1, t2, asList(TableChange.createDrop("x"), TableChange.createAdd("y")), NO_CHANGES, ChangeLevel.TABLE);
    }

    @Test
    public void addAndDropMultipleColumnAndIndex() {
        Table t1 = table(builder(TABLE_NAME).colBigInt("id").colDouble("d").colInt("l").colString("s", 32).
                key("d", "d").key("l", "l").uniqueKey("k", "l", "d").pk("id"));
        Table t2 = table(builder(TABLE_NAME).colBigInt("id").colDouble("d").colVarBinary("v", 32).colString("s", 64).
                key("d", "d").key("v", "v").uniqueKey("k", "v", "d").pk("id"));
        validate(
                t1, t2,
                asList(TableChange.createDrop("l"), TableChange.createModify("s", "s"), TableChange.createAdd("v")),
                asList(TableChange.createDrop("l"), TableChange.createAdd("v"), TableChange.createModify("k", "k")),
                ChangeLevel.TABLE,
                asList(changeDesc(TABLE_NAME, TABLE_NAME, false, ParentChange.NONE, "PRIMARY", "PRIMARY", "d", "d"))
        );
    }

    //
    // Auto index changes
    //

    @Test
    public void addDropAndModifyIndexAutoChanges() {
        Table t1 = table(builder(TABLE_NAME).colInt("c1").colInt("c2").colInt("c3").key("c1", "c1").key("c3", "c3"));
        Table t2 = table(builder(TABLE_NAME).colInt("c1").colInt("c2").colString("c3", 32).key("c2", "c2").key("c3", "c3"));
        validate(
                t1, t2,
                asList(TableChange.createModify("c3", "c3")),
                AUTO_CHANGES,
                ChangeLevel.TABLE,
                asList(changeDesc(TABLE_NAME, TABLE_NAME, false, ParentChange.NONE, "PRIMARY", "PRIMARY")),
                false,
                false,
                NO_INDEX_CHANGE,
                NO_IDENTITY_CHANGE
        );
        assertEquals("index changes", 3, AUTO_CHANGES.size());
    }

    //
    // Group Index changes
    //

    @Test
    public void dropColumnInGroupIndex() {
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA, typesTranslator);
        builder.table("p").colInt("id").colInt("x").pk("id")
               .table(TABLE).colInt("id").colInt("pid").colInt("y").pk("id").joinTo(SCHEMA, "p", "fk").on("pid", "id")
               .groupIndex("x_y", Index.JoinType.LEFT).on(TABLE, "y").and("p", "x");
        Table t1 = builder.unvalidatedAIS().getTable(TABLE_NAME);
        builder = AISBBasedBuilder.create(SCHEMA, typesTranslator);
        builder.table("p").colInt("id").colInt("x").pk("id")
               .table(TABLE).colInt("id").colInt("pid").pk("id").joinTo(SCHEMA, "p", "fk").on("pid", "id");
        Table t2 = builder.unvalidatedAIS().getTable(TABLE_NAME);
        final String KEY1 = Index.PRIMARY_KEY_CONSTRAINT;
        final String KEY2 = "__akiban_fk";
        validate(
                t1, t2,
                asList(TableChange.createDrop("y")),
                NO_CHANGES,
                ChangeLevel.TABLE,
                asList(changeDesc(TABLE_NAME, TABLE_NAME, false, ParentChange.NONE, KEY1, KEY1, KEY2, KEY2)),
                false,
                false,
                new String[]{ "[x_y]", EMPTY_MAP_STR, EMPTY_MAP_STR },
                NO_IDENTITY_CHANGE
        );
    }

    @Test
    public void dropGFKFrommMiddleWithGroupIndexes() {
        TableName iName = new TableName(SCHEMA, "i");
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA, typesTranslator);
        builder.table("p").colInt("id").colInt("x").pk("id")
               .table(TABLE).colInt("id").colInt("pid").colInt("y").pk("id").joinTo(SCHEMA, "p", "fk1").on("pid", "id")
               .table(iName).colInt("id").colInt("tid").colInt("z").pk("id").joinTo(SCHEMA, TABLE, "fk2").on("tid", "id")
               .groupIndex("x_y", Index.JoinType.LEFT).on(TABLE, "y").and("p", "x")                  // spans 2
               .groupIndex("x_y_z", Index.JoinType.LEFT).on("i", "z").and(TABLE, "y").and("p", "x"); // spans 3
        Table t1 = builder.unvalidatedAIS().getTable(TABLE_NAME);
        builder = AISBBasedBuilder.create(SCHEMA, typesTranslator);
        builder.table("p").colInt("id").colInt("x").pk("id")
                .table(TABLE).colInt("id").colInt("pid").colInt("y").pk("id").key("__akiban_fk1", "pid")
                .table(iName).colInt("id").colInt("tid").colInt("z").pk("id").joinTo(SCHEMA, TABLE, "fk2").on("tid", "id");
        Table t2 = builder.unvalidatedAIS().getTable(TABLE_NAME);
        validate(
                t1, t2,
                NO_CHANGES,
                NO_CHANGES,
                ChangeLevel.GROUP,
                asList(changeDesc(TABLE_NAME, TABLE_NAME, true, ParentChange.DROP), changeDesc(iName, iName, true, ParentChange.UPDATE)),
                true,
                false,
                new String[]{ "[x_y, x_y_z]", EMPTY_MAP_STR, EMPTY_MAP_STR },
                NO_IDENTITY_CHANGE
        );
    }

    //
    // FK change
    //

    private void fkTest(boolean isAdd) {
        TableName PARENT = new TableName(SCHEMA, "p");
        Table t1 = table(builder(PARENT).colBigInt("pid").pk("pid").table(TABLE).colBigInt("cid").colBigInt("pid").pk("cid"), TABLE_NAME);
        NewAISBuilder builder = builder(PARENT).colBigInt("pid").pk("pid").table(TABLE).colBigInt("cid").colBigInt("pid").pk("cid");
        AkibanInformationSchema ais = builder.unvalidatedAIS();
        Table t2 = ais.getTable(TABLE_NAME);
        Table p2 = ais.getTable(PARENT);
        ForeignKey.create(ais, "__fk_1", t2, asList(t2.getColumn("pid")), p2, asList(p2.getColumn("pid")), Action.RESTRICT, Action.RESTRICT, false, false);
        Table pre = isAdd ? t1 : t2;
        Table post = isAdd ? t2 : t1;
        validate(pre, post,
                 NO_CHANGES, NO_CHANGES, ChangeLevel.METADATA_CONSTRAINT,
                 asList(changeDesc(TABLE_NAME, TABLE_NAME, false, ParentChange.NONE, "PRIMARY", "PRIMARY"),
                        changeDesc(PARENT, PARENT, false, ParentChange.NONE, "PRIMARY", "PRIMARY")),
                 false, false, NO_INDEX_CHANGE, NO_IDENTITY_CHANGE);
    }

    @Test
    public void addFK() {
        fkTest(true);
    }

    @Test
    public void dropFK() {
        fkTest(false);
    }
}
