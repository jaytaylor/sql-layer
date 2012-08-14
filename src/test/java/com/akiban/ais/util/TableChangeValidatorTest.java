/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.ais.util;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.CharsetAndCollation;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.ais.model.aisb2.NewUserTableBuilder;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.akiban.ais.util.ChangedTableDescription.ParentChange;
import static com.akiban.ais.util.TableChangeValidator.ChangeLevel;
import static com.akiban.ais.util.TableChangeValidatorException.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TableChangeValidatorTest {
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";
    private static final TableName TABLE_NAME = new TableName(SCHEMA, TABLE);
    private static final List<TableChange> NO_CHANGES = null;
    private static final Collection<IndexName> NO_INDEX_CHANGE = Collections.emptySet();


    private static NewUserTableBuilder builder(TableName name) {
        return AISBBasedBuilder.create(SCHEMA).userTable(name);
    }

    private UserTable table(NewAISBuilder builder) {
        AkibanInformationSchema ais = builder.ais();
        assertEquals("User table count", 1, ais.getUserTables().size());
        return ais.getUserTables().values().iterator().next();
    }

    private UserTable table(NewAISBuilder builder, TableName tableName) {
        UserTable table = builder.ais().getUserTable(tableName);
        assertNotNull("Found table: " + tableName, table);
        return table;
    }

    private static TableChangeValidator validate(UserTable t1, UserTable t2,
                                                 List<TableChange> columnChanges, List<TableChange> indexChanges,
                                                 ChangeLevel expectedChangeLevel) {
        return validate(t1, t2, columnChanges, indexChanges, expectedChangeLevel,
                        asList(changeDesc(TABLE_NAME, TABLE_NAME, false, ParentChange.NONE, "PRIMARY", "PRIMARY")),
                        false, false, NO_INDEX_CHANGE, false);
    }

    private static TableChangeValidator validate(UserTable t1, UserTable t2,
                                                 List<TableChange> columnChanges, List<TableChange> indexChanges,
                                                 ChangeLevel expectedChangeLevel,
                                                 List<String> expectedChangedTables) {
        return validate(t1, t2, columnChanges, indexChanges, expectedChangeLevel,
                        expectedChangedTables,
                        false, false, NO_INDEX_CHANGE, false);
    }

    private static TableChangeValidator validate(UserTable t1, UserTable t2,
                                                 List<TableChange> columnChanges, List<TableChange> indexChanges,
                                                 ChangeLevel expectedChangeLevel,
                                                 List<String> expectedChangedTables,
                                                 boolean expectedParentChange,
                                                 boolean expectedPrimaryKeyChange,
                                                 Collection<IndexName> expectedAutoGroupIndexChange,
                                                 boolean autoIndexChanges) {
        TableChangeValidator validator = new TableChangeValidator(t1, t2, columnChanges, indexChanges, autoIndexChanges);
        validator.compareAndThrowIfNecessary();
        assertEquals("Final change level", expectedChangeLevel, validator.getFinalChangeLevel());
        assertEquals("Parent changed", expectedParentChange, validator.isParentChanged());
        assertEquals("Primary key changed", expectedPrimaryKeyChange, validator.isPrimaryKeyChanged());
        assertEquals("Changed tables", expectedChangedTables.toString(), validator.getAllChangedTables().toString());
        assertEquals("Auto group index changes", expectedAutoGroupIndexChange.toString(), validator.getAutoAffectedGroupIndexes().toString());
        assertEquals("Unmodified changes", "[]", validator.getUnmodifiedChanges().toString());
        return validator;
    }

    private static Map<String,String> map(String... pairs) {
        assertTrue("Even number of pairs", (pairs.length % 2) == 0);
        Map<String,String> map = new TreeMap<String, String>();
        for(int i = 0; i < pairs.length; i += 2) {
            map.put(pairs[i], pairs[i+1]);
        }
        return map;
    }

    private static String changeDesc(TableName oldName, TableName newName, boolean newGroup, ParentChange parentChange, String... indexPairs) {
        return ChangedTableDescription.toString(oldName, newName, newGroup, parentChange, map(indexPairs));
    }


    //
    // Table
    //

    @Test
    public void sameTable() {
        UserTable t = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        validate(t, t, NO_CHANGES, NO_CHANGES, ChangeLevel.NONE);
    }

    @Test
    public void unchangedTable() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        validate(t1, t2, NO_CHANGES, NO_CHANGES, ChangeLevel.NONE);
    }

    @Test
    public void changeOnlyTableName() {
        TableName name2 = new TableName("x", "y");
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        UserTable t2 = table(builder(name2).colBigInt("id").pk("id"));
        validate(t1, t2, NO_CHANGES, NO_CHANGES, ChangeLevel.METADATA,
                 asList(changeDesc(TABLE_NAME, name2, false, ParentChange.NONE, "PRIMARY", "PRIMARY")));
    }

    @Test
    public void changeDefaultCharset() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        t1.setCharsetAndCollation(CharsetAndCollation.intern("utf8", "binary"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        t1.setCharsetAndCollation(CharsetAndCollation.intern("utf16", "binary"));
        validate(t1, t2, NO_CHANGES, NO_CHANGES, ChangeLevel.METADATA);
    }

    @Test
    public void changeDefaultCollation() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        t1.setCharsetAndCollation(CharsetAndCollation.intern("utf8", "binary"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        t1.setCharsetAndCollation(CharsetAndCollation.intern("utf8", "utf8_general_ci"));
        validate(t1, t2, NO_CHANGES, NO_CHANGES, ChangeLevel.METADATA);
    }

    //
    // Column
    //

    @Test
    public void addColumn() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        validate(t1, t2, asList(TableChange.createAdd("x")), NO_CHANGES, ChangeLevel.TABLE);
    }

    @Test
    public void dropColumn() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        validate(t1, t2, asList(TableChange.createDrop("x")), NO_CHANGES, ChangeLevel.TABLE);
    }

    @Test
    public void modifyColumnDataType() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("y").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colString("y", 32).pk("id"));
        validate(t1, t2, asList(TableChange.createModify("y", "y")), NO_CHANGES, ChangeLevel.TABLE);
    }

    @Test
    public void modifyColumnName() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("y").pk("id"));
        validate(t1, t2, asList(TableChange.createModify("x", "y")), NO_CHANGES, ChangeLevel.METADATA);
    }

    @Test
    public void modifyColumnNotNullToNull() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x", false).pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x", true).pk("id"));
        validate(t1, t2, asList(TableChange.createModify("x", "x")), NO_CHANGES, ChangeLevel.METADATA);
    }

    @Test
    public void modifyColumnNullToNotNull() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x", true).pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x", false).pk("id"));
        validate(t1, t2, asList(TableChange.createModify("x", "x")), NO_CHANGES, ChangeLevel.METADATA_NOT_NULL);
    }

    @Test
    public void modifyAddGeneratedBy() {
        final TableName SEQ_NAME = new TableName(SCHEMA, "seq-1");
        UserTable t1 = table(builder(TABLE_NAME).colLong("id", false).pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colLong("id", false).pk("id").sequence(SEQ_NAME.getTableName()));
        t2.getColumn("id").setIdentityGenerator(t2.getAIS().getSequence(SEQ_NAME));
        t2.getColumn("id").setDefaultIdentity(true);
        validate(t1, t2, asList(TableChange.createModify("id", "id")), NO_CHANGES, ChangeLevel.METADATA);
    }

    @Test
    public void modifyDropGeneratedBy() {
        final TableName SEQ_NAME = new TableName(SCHEMA, "seq-1");
        UserTable t1 = table(builder(TABLE_NAME).colLong("id", false).pk("id").sequence(SEQ_NAME.getTableName()));
        t1.getColumn("id").setIdentityGenerator(t1.getAIS().getSequence(SEQ_NAME));
        t1.getColumn("id").setDefaultIdentity(true);
        UserTable t2 = table(builder(TABLE_NAME).colLong("id", false).pk("id"));
        validate(t1, t2, asList(TableChange.createModify("id", "id")), NO_CHANGES, ChangeLevel.METADATA);
    }

    @Test
    public void modifyColumnAddDefault() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x", false).colLong("c1", true).pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x", false).colLong("c1", true).pk("id"));
        t2.getColumn("c1").setDefaultValue("42");
        validate(t1, t2, asList(TableChange.createModify("c1", "c1")), NO_CHANGES, ChangeLevel.METADATA);
    }

    @Test
    public void modifyColumnDropDefault() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x", false).colLong("c1", true).pk("id"));
        t1.getColumn("c1").setDefaultValue("42");
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x", false).colLong("c1", true).pk("id"));
        validate(t1, t2, asList(TableChange.createModify("c1", "c1")), NO_CHANGES, ChangeLevel.METADATA);
    }

    //
    // Column (negative)
    //

    @Test(expected=UndeclaredColumnChangeException.class)
    public void addColumnUnspecified() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        validate(t1, t2, NO_CHANGES, NO_CHANGES, null);
    }

    @Test(expected=UnchangedColumnNotPresentException.class)
    public void dropColumnUnspecified() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        validate(t1, t2, NO_CHANGES, NO_CHANGES, null);
    }

    @Test(expected=DropColumnNotPresentException.class)
    public void dropColumnUnknown() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        validate(t1, t2, asList(TableChange.createDrop("x")), NO_CHANGES, null);
    }

    @Test
    public void modifyColumnNotChanged() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        TableChangeValidator tcv = new TableChangeValidator(t1, t2, asList(TableChange.createModify("x", "x")), NO_CHANGES, false);
        tcv.compareAndThrowIfNecessary();
        assertEquals("Final change level", ChangeLevel.NONE, tcv.getFinalChangeLevel());
        assertEquals("Unmodified change count", 1, tcv.getUnmodifiedChanges().size());
    }

    @Test(expected=ModifyColumnNotPresentException.class)
    public void modifyColumnUnknown() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        validate(t1, t2, asList(TableChange.createModify("y", "y")), NO_CHANGES, null);
    }

    @Test(expected=UndeclaredColumnChangeException.class)
    public void modifyColumnUnspecified() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colString("x", 32).pk("id"));
        validate(t1, t2, NO_CHANGES, NO_CHANGES, null);
    }

    //
    // Index
    //

    @Test
    public void addIndex() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        validate(t1, t2, NO_CHANGES, asList(TableChange.createAdd("x")), ChangeLevel.INDEX);
    }

    @Test
    public void dropIndex() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        validate(t1, t2, NO_CHANGES, asList(TableChange.createDrop("x")), ChangeLevel.INDEX);
    }

    @Test
    public void modifyIndexedColumn() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").colBigInt("y").key("k", "x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").colBigInt("y").key("k", "y").pk("id"));
        validate(t1, t2, NO_CHANGES, asList(TableChange.createModify("k", "k")), ChangeLevel.INDEX);
    }

    @Test
    public void modifyIndexedType() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colString("x", 32).key("x", "x").pk("id"));
        validate(t1, t2, asList(TableChange.createModify("x", "x")), asList(TableChange.createModify("x", "x")),
                 ChangeLevel.TABLE);
    }

    @Test
    public void modifyIndexName() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("a", "x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("b", "x").pk("id"));
        validate(t1, t2, NO_CHANGES, asList(TableChange.createModify("a", "b")), ChangeLevel.METADATA);
    }

    //
    // Index (negative)
    //

    @Test(expected=UndeclaredIndexChangeException.class)
    public void addIndexUnspecified() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        validate(t1, t2, NO_CHANGES, NO_CHANGES, null);
    }

    @Test(expected=UnchangedIndexNotPresentException.class)
    public void dropIndexUnspecified() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        validate(t1, t2, NO_CHANGES, NO_CHANGES, null);
    }

    @Test(expected=DropIndexNotPresentException.class)
    public void dropIndexUnknown() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        validate(t1, t2, NO_CHANGES, asList(TableChange.createDrop("x")), null);
    }

    @Test
    public void modifyIndexNotChanged() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        TableChangeValidator tcv = new TableChangeValidator(t1, t2, NO_CHANGES, asList(TableChange.createModify("x", "x")), false);
        tcv.compareAndThrowIfNecessary();
        assertEquals("Final change level", ChangeLevel.NONE, tcv.getFinalChangeLevel());
        assertEquals("Unmodified change count", 1, tcv.getUnmodifiedChanges().size());
    }

    @Test(expected=ModifyIndexNotPresentException.class)
    public void modifyIndexUnknown() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        validate(t1, t2, NO_CHANGES, asList(TableChange.createModify("y", "y")), null);
    }

    @Test(expected=UndeclaredIndexChangeException.class)
    public void modifyIndexUnspecified() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").colBigInt("y").key("k", "x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").colBigInt("y").key("k", "y").pk("id"));
        validate(t1, t2, NO_CHANGES, NO_CHANGES, null);
    }

    @Test(expected=UndeclaredIndexChangeException.class)
    public void modifyIndexedColumnIndexUnspecified() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colString("x", 32).key("x", "x").pk("id"));
        validate(t1, t2, asList(TableChange.createModify("x", "x")), NO_CHANGES, null);
    }

    //
    // Group
    //

    @Test
    public void modifyPKColumnTypeSingleTableGroup() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colString("id", 32).pk("id"));
        validate(t1, t2,
                 asList(TableChange.createModify("id", "id")),
                 asList(TableChange.createModify(Index.PRIMARY_KEY_CONSTRAINT, Index.PRIMARY_KEY_CONSTRAINT)),
                 ChangeLevel.GROUP,
                 asList(changeDesc(TABLE_NAME, TABLE_NAME, false, ParentChange.NONE)),
                 false, true, NO_INDEX_CHANGE, false);
    }

    @Test
    public void dropPrimaryKeySingleTableGroup() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id"));
        validate(t1, t2,
                 NO_CHANGES,
                 asList(TableChange.createDrop(Index.PRIMARY_KEY_CONSTRAINT)),
                 ChangeLevel.GROUP,
                 asList(changeDesc(TABLE_NAME, TABLE_NAME, false, ParentChange.NONE)),
                 false, true, NO_INDEX_CHANGE, false);
    }

    @Test
    public void dropParentJoinTwoTableGroup() {
        TableName parentName = new TableName(SCHEMA, "parent");
        UserTable t1 = table(
                builder(parentName).colLong("id").pk("id").
                        userTable(TABLE_NAME).colBigInt("id").colLong("pid").pk("id").joinTo(SCHEMA, "parent", "fk").on("pid", "id"),
                TABLE_NAME
        );
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colLong("pid").pk("id"));
        validate(t1, t2,
                 NO_CHANGES,
                 asList(TableChange.createDrop("__akiban_fk")),
                 ChangeLevel.GROUP,
                 asList(changeDesc(TABLE_NAME, TABLE_NAME, true, ParentChange.DROP)),
                 true, false, NO_INDEX_CHANGE, false);
    }

    @Test
    public void dropPrimaryKeyMiddleOfGroup() {
        TableName cName = new TableName(SCHEMA, "c");
        TableName oName = new TableName(SCHEMA, "o");
        TableName iName = new TableName(SCHEMA, "i");
        NewAISBuilder builder1 = AISBBasedBuilder.create();
        builder1.userTable(cName).colBigInt("id", false).pk("id")
                .userTable(oName).colBigInt("id", false).colBigInt("cid", true).pk("id").joinTo(SCHEMA, "c", "fk1").on("cid", "id")
                .userTable(iName).colBigInt("id", false).colBigInt("oid", true).pk("id").joinTo(SCHEMA, "o", "fk2").on("oid", "id");
        NewAISBuilder builder2 = AISBBasedBuilder.create();
        builder2.userTable(cName).colBigInt("id", false).pk("id")
                .userTable(oName).colBigInt("id", false).colBigInt("cid", true).joinTo(SCHEMA, "c", "fk1").on("cid", "id")
                .userTable(iName).colBigInt("id", false).colBigInt("oid", true).pk("id").joinTo(SCHEMA, "o", "fk2").on("oid", "id");
        UserTable t1 = builder1.unvalidatedAIS().getUserTable(oName);
        UserTable t2 = builder2.unvalidatedAIS().getUserTable(oName);
        validate(
                t1, t2,
                NO_CHANGES,
                asList(TableChange.createDrop(Index.PRIMARY_KEY_CONSTRAINT)),
                ChangeLevel.GROUP,
                asList(
                        changeDesc(oName, oName, false, ParentChange.NONE),
                        changeDesc(iName, iName, true, ParentChange.DROP)
                ),
                false,
                true,
                NO_INDEX_CHANGE,
                false
        );
    }

    //
    // Multi-part
    //

    @Test
    public void addAndDropColumn() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("y").pk("id"));
        validate(t1, t2, asList(TableChange.createDrop("x"), TableChange.createAdd("y")), NO_CHANGES, ChangeLevel.TABLE);
    }

    @Test
    public void addAndDropMultipleColumnAndIndex() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colDouble("d").colLong("l").colString("s", 32).
                key("d", "d").key("l", "l").uniqueKey("k", "l", "d").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colDouble("d").colVarBinary("v", 32).colString("s", 64).
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
        UserTable t1 = table(builder(TABLE_NAME).colLong("c1").colLong("c2").colLong("c3").key("c1", "c1").key("c3", "c3"));
        UserTable t2 = table(builder(TABLE_NAME).colLong("c1").colLong("c2").colString("c3", 32).key("c2", "c2").key("c3", "c3"));
        validate(
                t1, t2,
                asList(TableChange.createModify("c3", "c3")),
                NO_CHANGES,
                ChangeLevel.TABLE,
                asList(changeDesc(TABLE_NAME, TABLE_NAME, false, ParentChange.NONE)),
                false,
                false,
                NO_INDEX_CHANGE,
                true
        );
    }
}
