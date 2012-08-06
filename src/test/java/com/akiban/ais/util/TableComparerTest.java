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
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewUserTableBuilder;
import org.junit.Test;

import java.util.List;

import static com.akiban.ais.util.TableComparer.ChangeLevel;
import static com.akiban.ais.util.TableComparerExceptions.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class TableComparerTest {
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";
    private static final TableName TABLE_NAME = new TableName(SCHEMA, TABLE);
    private static final List<TableChange> NO_CHANGES = null;


    private static NewUserTableBuilder builder(TableName name) {
        return AISBBasedBuilder.create().userTable(name);
    }

    private UserTable table(NewUserTableBuilder builder) {
        AkibanInformationSchema ais = builder.ais();
        assertEquals("User table count", 1, ais.getUserTables().size());
        return ais.getUserTables().values().iterator().next();
    }


    private static TableComparer uncheckedCompare(UserTable t1, UserTable t2,
                                                  List<TableChange> colChanges, List<TableChange> idxChanges,
                                                  ChangeLevel finalChangeLevel) {
        TableComparer comparer = new TableComparer(t1, t2, colChanges, idxChanges);
        comparer.compare();
        assertEquals("Final change level", finalChangeLevel, comparer.getTableChangeLevel());
        return comparer;
    }

    private static TableComparer checkCompare(UserTable t1, UserTable t2,
                                              List<TableChange> colChanges, List<TableChange> idxChanges,
                                              ChangeLevel finalChangeLevel) {
        TableComparer comparer = uncheckedCompare(t1, t2, colChanges, idxChanges, finalChangeLevel);
        comparer.compareAndThrowIfNecessary();
        return comparer;
    }


    //
    // Table
    //

    @Test
    public void sameTable() {
        UserTable t = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        checkCompare(t, t, NO_CHANGES, NO_CHANGES, ChangeLevel.NONE);
    }

    @Test
    public void unchangedTable() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        checkCompare(t1, t2, NO_CHANGES, NO_CHANGES, ChangeLevel.NONE);
    }

    @Test
    public void changeOnlyTableName() {
        TableName name2 = new TableName("x", "y");
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        UserTable t2 = table(builder(name2).colBigInt("id").pk("id"));
        checkCompare(t1, t2, NO_CHANGES, NO_CHANGES, ChangeLevel.METADATA);
    }

    //
    // Column
    //

    @Test
    public void addColumn() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        checkCompare(t1, t2, asList(TableChange.createAdd("x")), NO_CHANGES, ChangeLevel.TABLE);
    }

    @Test
    public void dropColumn() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        checkCompare(t1, t2, asList(TableChange.createDrop("x")), NO_CHANGES, ChangeLevel.TABLE);
    }

    @Test
    public void modifyColumnDataType() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("y").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colString("y", 32).pk("id"));
        checkCompare(t1, t2, asList(TableChange.createModify("y", "y")), NO_CHANGES, ChangeLevel.TABLE);
    }

    @Test
    public void modifyColumnName() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("y").pk("id"));
        checkCompare(t1, t2, asList(TableChange.createModify("x", "y")), NO_CHANGES, ChangeLevel.METADATA);
    }

    @Test
    public void modifyColumnNullability() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x", false).pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x", true).pk("id"));
        checkCompare(t1, t2, asList(TableChange.createModify("x", "x")), NO_CHANGES, ChangeLevel.METADATA_NULL);
    }

    //
    // Column (negative)
    //

    @Test(expected=UndeclaredColumnChangeException.class)
    public void addColumnUnspecified() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        checkCompare(t1, t2, NO_CHANGES, NO_CHANGES, null);
    }

    @Test(expected=UnchangedColumnNotPresentException.class)
    public void dropColumnUnspecified() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        checkCompare(t1, t2, NO_CHANGES, NO_CHANGES, null);
    }

    @Test(expected=DropColumnNotPresentException.class)
    public void dropColumnUnknown() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        checkCompare(t1, t2, asList(TableChange.createDrop("x")), NO_CHANGES, null);
    }

    @Test(expected=ModifyColumnNotChangedException.class)
    public void modifyColumnNotChanged() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        checkCompare(t1, t2, asList(TableChange.createModify("x", "x")), NO_CHANGES, null);
    }

    @Test(expected=ModifyColumnNotPresentException.class)
    public void modifyColumnUnknown() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        checkCompare(t1, t2, asList(TableChange.createModify("y", "y")), NO_CHANGES, null);
    }

    @Test(expected=UndeclaredColumnChangeException.class)
    public void modifyColumnUnspecified() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colString("x", 32).pk("id"));
        checkCompare(t1, t2, NO_CHANGES, NO_CHANGES, null);
    }

    //
    // Index
    //

    @Test
    public void addIndex() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        checkCompare(t1, t2, NO_CHANGES, asList(TableChange.createAdd("x")), ChangeLevel.INDEX);
    }

    @Test
    public void dropIndex() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        checkCompare(t1, t2, NO_CHANGES, asList(TableChange.createDrop("x")), ChangeLevel.INDEX);
    }

    @Test
    public void modifyIndexedColumn() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").colBigInt("y").key("k", "x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").colBigInt("y").key("k", "y").pk("id"));
        checkCompare(t1, t2, NO_CHANGES, asList(TableChange.createModify("k", "k")), ChangeLevel.INDEX);
    }

    @Test
    public void modifyIndexedType() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colString("x", 32).key("x", "x").pk("id"));
        checkCompare(t1, t2,asList(TableChange.createModify("x", "x")), asList(TableChange.createModify("x", "x")), ChangeLevel.TABLE);
    }

    @Test
    public void modifyIndexName() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("a", "x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("b", "x").pk("id"));
        checkCompare(t1, t2, NO_CHANGES, asList(TableChange.createModify("a", "b")), ChangeLevel.METADATA);
    }

    //
    // Index (negative)
    //

    @Test(expected=UndeclaredIndexChangeException.class)
    public void addIndexUnspecified() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        checkCompare(t1, t2, NO_CHANGES, NO_CHANGES, null);
    }

    @Test(expected=UnchangedIndexNotPresentException.class)
    public void dropIndexUnspecified() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        checkCompare(t1, t2, NO_CHANGES, NO_CHANGES, null);
    }

    @Test(expected=DropIndexNotPresentException.class)
    public void dropIndexUnknown() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        checkCompare(t1, t2, NO_CHANGES, asList(TableChange.createDrop("x")), null);
    }

    @Test(expected=ModifyIndexNotChangedException.class)
    public void modifyIndexNotChanged() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").key("x", "x").pk("id"));
        checkCompare(t1, t2, NO_CHANGES, asList(TableChange.createModify("x", "x")), null);
    }

    @Test(expected=ModifyIndexNotPresentException.class)
    public void modifyIndexUnknown() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").pk("id"));
        checkCompare(t1, t2, NO_CHANGES, asList(TableChange.createModify("y", "y")), null);
    }

    @Test(expected=UndeclaredIndexChangeException.class)
    public void modifyIndexUnspecified() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").colBigInt("y").key("k", "x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").colBigInt("y").key("k", "y").pk("id"));
        checkCompare(t1, t2, NO_CHANGES, NO_CHANGES, null);
    }

    //
    // Multi-part
    //

    @Test
    public void addAndDropColumn() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("x").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colBigInt("y").pk("id"));
        checkCompare(t1, t2, asList(TableChange.createDrop("x"), TableChange.createAdd("y")), NO_CHANGES, ChangeLevel.TABLE);
    }

    @Test
    public void addAndDropMultipleColumnAndIndex() {
        UserTable t1 = table(builder(TABLE_NAME).colBigInt("id").colDouble("d").colLong("l").colString("s", 32).
                key("d", "d").key("l", "l").uniqueKey("k", "l", "d").pk("id"));
        UserTable t2 = table(builder(TABLE_NAME).colBigInt("id").colDouble("d").colVarBinary("v", 32).colString("s", 64).
                key("d", "d").key("v", "v").uniqueKey("k", "v", "d").pk("id"));
        checkCompare(t1, t2,
                     asList(TableChange.createDrop("l"), TableChange.createModify("s", "s"), TableChange.createAdd("v")),
                     asList(TableChange.createDrop("l"), TableChange.createAdd("v"), TableChange.createModify("k", "k")),
                     ChangeLevel.TABLE);
    }
}
