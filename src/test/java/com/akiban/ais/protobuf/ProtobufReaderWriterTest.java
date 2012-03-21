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
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

public class ProtobufReaderWriterTest {

    @Test
    public void empty() {
        final AkibanInformationSchema inAIS = new AkibanInformationSchema();
        inAIS.freeze();
        final AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS);
    }

    @Test
    public void caoi() {
        final AkibanInformationSchema inAIS = CAOIBuilderFiller.createAndFillBuilder("test").ais();
        final AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS);
    }
    
    @Test
    public void caoiWithGroupIndex() {
        NewAISBuilder builder = CAOIBuilderFiller.createAndFillBuilder("test");
        builder.groupIndex("iprice_odate", Index.JoinType.RIGHT).
                on(CAOIBuilderFiller.ITEM_TABLE, "unit_price").
                and(CAOIBuilderFiller.ORDER_TABLE, "order_date");
        
        final AkibanInformationSchema inAIS = builder.ais();
        final AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS);
    }

    @Test
    public void nonDefaultCharsetAndCollations() {
        final AkibanInformationSchema inAIS = new AkibanInformationSchema("utf32", "utf32_polish_ci");
        CAOIBuilderFiller.createAndFillBuilder(inAIS, "test").ais(false);
        inAIS.getUserTable("test", CAOIBuilderFiller.ORDER_TABLE).
                setCharsetAndCollation(CharsetAndCollation.intern("utf16", "utf16_slovak_ci"));
        inAIS.getUserTable("test", CAOIBuilderFiller.CUSTOMER_TABLE).getColumn("customer_name").
                setCharsetAndCollation(CharsetAndCollation.intern("ujis", "ujis_japanese_ci"));
        inAIS.freeze();
        
        final AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS);
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
