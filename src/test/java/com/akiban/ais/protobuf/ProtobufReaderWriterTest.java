/**
 * Copyright (C) 2012 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.ais.protobuf;

import com.akiban.ais.CAOIBuilderFiller;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.CharsetAndCollation;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Types;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.server.error.ProtobufReadException;
import com.akiban.server.error.ProtobufWriteException;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

public class ProtobufReaderWriterTest {
    private final String SCHEMA = "test";

    @Test
    public void empty() {
        final AkibanInformationSchema inAIS = new AkibanInformationSchema();
        inAIS.freeze();
        final AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS);
    }

    @Test
    public void caoi() {
        final AkibanInformationSchema inAIS = CAOIBuilderFiller.createAndFillBuilder(SCHEMA).ais();
        final AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS);
    }
    
    @Test
    public void caoiWithGroupIndex() {
        NewAISBuilder builder = CAOIBuilderFiller.createAndFillBuilder(SCHEMA);
        builder.groupIndex("iprice_odate", Index.JoinType.RIGHT).
                on(CAOIBuilderFiller.ITEM_TABLE, "unit_price").
                and(CAOIBuilderFiller.ORDER_TABLE, "order_date");
        
        final AkibanInformationSchema inAIS = builder.ais();
        final AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS);
    }

    @Test
    public void nonDefaultCharsetAndCollations() {
        // AIS char/col not serialized (will be on Schema when that exists)
        final AkibanInformationSchema inAIS = CAOIBuilderFiller.createAndFillBuilder(SCHEMA).ais(false);
        inAIS.getUserTable(SCHEMA, CAOIBuilderFiller.ORDER_TABLE).
                setCharsetAndCollation(CharsetAndCollation.intern("utf16", "utf16_slovak_ci"));
        inAIS.getUserTable(SCHEMA, CAOIBuilderFiller.CUSTOMER_TABLE).getColumn("customer_name").
                setCharsetAndCollation(CharsetAndCollation.intern("ujis", "ujis_japanese_ci"));
        inAIS.freeze();
        
        final AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS);
    }

    /*
     * Stubbed out parent, similar to how table creation from the adapter works
     */
    @Test
    public void partialParentWithFullChild() {
        final AkibanInformationSchema inAIS = new AkibanInformationSchema();
        
        UserTable stubCustomer = UserTable.create(inAIS, SCHEMA, "c", 1);
        Column cId = Column.create(stubCustomer, "id", 2, Types.BIGINT, false, null, null, null, null);

        UserTable realOrder = UserTable.create(inAIS, SCHEMA, "o", 2);
        Column oId = Column.create(realOrder, "oid", 0, Types.BIGINT, false, null, null, null, null);
        Column oCid = Column.create(realOrder, "cid", 1, Types.BIGINT, false, null, null, null, null);
        Column.create(realOrder, "odate", 2, Types.DATE, true, null, null, null, null);
        Index orderPK = TableIndex.create(inAIS, realOrder, Index.PRIMARY_KEY_CONSTRAINT, 0, true, Index.PRIMARY_KEY_CONSTRAINT);
        IndexColumn.create(orderPK, oId, 0, true, null);
        Index akFk = TableIndex.create(inAIS, realOrder, Index.GROUPING_FK_PREFIX + "_fk1", 1, false, Index.FOREIGN_KEY_CONSTRAINT);
        IndexColumn.create(akFk, oCid, 0, true, null);
        Join coJoin = Join.create(inAIS, "co", stubCustomer, realOrder);
        JoinColumn.create(coJoin, cId, oCid);

        final AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS);
    }

    /*
     * Stubbed out table, similar to how index creation from the adapter works
     */
    @Test
    public void partialTableWithIndexes() {
        final AkibanInformationSchema inAIS = new AkibanInformationSchema();

        UserTable stubCustomer = UserTable.create(inAIS, SCHEMA, "c", 1);
        Column cFirstName = Column.create(stubCustomer, "first_name", 3, Types.VARCHAR, true, 32L, null, null, null);
        Column cLastName = Column.create(stubCustomer, "last_name", 4, Types.VARCHAR, true, 32L, null, null, null);
        Column cPayment = Column.create(stubCustomer, "payment", 6, Types.INT, true, null, null, null, null);
        Index iName = TableIndex.create(inAIS, stubCustomer, "name", 2, false, Index.KEY_CONSTRAINT);
        IndexColumn.create(iName, cLastName, 0, true, null);
        IndexColumn.create(iName, cFirstName, 1, true, null);
        Index iPayment = TableIndex.create(inAIS, stubCustomer, "payment", 3, false, Index.KEY_CONSTRAINT);
        IndexColumn.create(iPayment, cPayment, 0, true, null);
        
        final AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS);

    }

    @Test(expected=ProtobufReadException.class)
    public void missingRootTable() {
        final AkibanInformationSchema inAIS = CAOIBuilderFiller.createAndFillBuilder(SCHEMA).ais(false);
        inAIS.getUserTables().remove(TableName.create(SCHEMA, CAOIBuilderFiller.CUSTOMER_TABLE));
        writeAndRead(inAIS);
    }

    @Test(expected=ProtobufReadException.class)
    public void readBufferTooSmall() {
        ByteBuffer bb = ByteBuffer.allocate(4096);
        final AkibanInformationSchema inAIS = CAOIBuilderFiller.createAndFillBuilder(SCHEMA).ais();
        ProtobufWriter writer = new ProtobufWriter(bb);
        writer.save(inAIS);

        bb.flip();
        bb.limit(bb.limit() / 2);
        ProtobufReader reader = new ProtobufReader(bb);
        reader.load();
    }

    @Test(expected=ProtobufWriteException.class)
    public void writeBufferTooSmall() {
        ByteBuffer bb = ByteBuffer.allocate(10);
        final AkibanInformationSchema inAIS = CAOIBuilderFiller.createAndFillBuilder(SCHEMA).ais();
        ProtobufWriter writer = new ProtobufWriter(bb);
        writer.save(inAIS);
    }


    private AkibanInformationSchema writeAndRead(AkibanInformationSchema inAIS) {
        ByteBuffer bb = createByteBuffer();

        ProtobufWriter writer = new ProtobufWriter(bb);
        writer.save(inAIS);

        bb.flip();
        ProtobufReader reader = new ProtobufReader(bb);
        AkibanInformationSchema outAIS = reader.load();

        return outAIS;
    }

    private ByteBuffer createByteBuffer() {
        return ByteBuffer.allocate(4096);
    }

    private void compareAndAssert(AkibanInformationSchema lhs, AkibanInformationSchema rhs) {
        assertEquals("AIS charsets", lhs.getCharsetAndCollation().charset(), rhs.getCharsetAndCollation().charset());
        assertEquals("AIS collations", lhs.getCharsetAndCollation().collation(), rhs.getCharsetAndCollation().collation());

        GroupMaps lhsGroups = new GroupMaps(lhs.getGroups().values());
        GroupMaps rhsGroups = new GroupMaps(rhs.getGroups().values());
        lhsGroups.compareAndAssert(rhsGroups);
        
        TableMaps lhsTables = new TableMaps(lhs.getUserTables().values());
        TableMaps rhsTables = new TableMaps(rhs.getUserTables().values());
        lhsTables.compareAndAssert(rhsTables);
    }

    private static class GroupMaps {
        public final Collection<String> names = new TreeSet<String>();
        public final Collection<String> indexes = new TreeSet<String>();
        
        public GroupMaps(Collection<Group> groups) {
            for(Group group : groups) {
                names.add(group.getName());
                for(Index index : group.getIndexes()) {
                    indexes.add(index.toString());
                }
            }
        }
        
        public void compareAndAssert(GroupMaps rhs) {
            assertEquals("Group names", names.toString(), rhs.names.toString());
            assertEquals("Group indexes", indexes.toString(), rhs.indexes.toString());
        }
    }

    private static class TableMaps {
        public final Collection<String> names = new TreeSet<String>();
        public final Collection<String> indexes = new TreeSet<String>();
        public final Collection<String> columns = new TreeSet<String>();
        public final Collection<String> charAndCols = new TreeSet<String>();
        
        public TableMaps(Collection<UserTable> tables) {
            for(UserTable table : tables) {
                names.add(table.getName().toString());
                for(Column column : table.getColumns()) {
                    columns.add(column.toString() + " " + column.getTypeDescription() + " " + column.getCharsetAndCollation());
                }
                for(Index index : table.getIndexes()) {
                    indexes.add(index.toString());
                }
                charAndCols.add(table.getName() + " " + table.getCharsetAndCollation().toString());
            }
        }
        
        public void compareAndAssert(TableMaps rhs) {
            assertEquals("Table names", names.toString(), rhs.names.toString());
            assertEquals("Table columns", columns.toString(), rhs.columns.toString());
            assertEquals("Table indexes", indexes.toString(), rhs.indexes.toString());
            assertEquals("Table charAndCols", charAndCols.toString(), rhs.charAndCols.toString());
        }
    }
}
