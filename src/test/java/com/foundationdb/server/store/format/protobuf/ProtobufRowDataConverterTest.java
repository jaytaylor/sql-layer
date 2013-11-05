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

package com.foundationdb.server.store.format.protobuf;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.protobuf.CommonProtobuf.ProtobufRowFormat;
import com.foundationdb.protobuf.ProtobufDecompiler;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.rowdata.SchemaFactory;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.UUID;

public class ProtobufRowDataConverterTest
{
    private static final String SCHEMA = "test";

    protected AkibanInformationSchema ais(String ddl) {
        AkibanInformationSchema ais = new SchemaFactory(SCHEMA).aisWithRowDefs(ddl);
        for (Table table : ais.getTables().values()) {
            if (!table.hasVersion()) {
                table.setVersion(0);
            }
            if (table.getUuid() == null)
                table.setUuid(UUID.randomUUID());
            for (Column column : table.getColumnsIncludingInternal()) {
                if (column.getUuid() == null)
                    column.setUuid(UUID.randomUUID());
            }
        }
        return ais;
    }

    protected ProtobufRowDataConverter converter(Group g) throws Exception {
        AISToProtobuf a2p = new AISToProtobuf(ProtobufRowFormat.Type.GROUP_MESSAGE);
        a2p.addGroup(g);
        FileDescriptorSet set = a2p.build();
        if (false) {
            new ProtobufDecompiler((Appendable)System.out).decompile(set);
        }
        FileDescriptor gdesc = FileDescriptor.buildFrom(set.getFile(0),
                                                        ProtobufStorageDescriptionHelper.DEPENDENCIES);
        return ProtobufRowDataConverter.forGroup(g, gdesc);
    }

    protected void encodeDecode(AkibanInformationSchema ais,
                                ProtobufRowDataConverter converter, 
                                RowDef rowDef, 
                                Object... values) 
            throws Exception {
        RowData rowDataIn = new RowData(new byte[128]);
        rowDataIn.createRow(rowDef, values, true);
        DynamicMessage msg = converter.encode(rowDataIn);
        if (false) {
            System.out.println(converter.shortFormat(msg));
        }
        RowData rowDataOut = new RowData(new byte[128]);
        converter.decode(msg, rowDataOut);
        assertEquals("rows match", rowDataIn.toString(ais), rowDataOut.toString(ais));
    }

    @Test
    public void testSimple() throws Exception {
        AkibanInformationSchema ais = ais(
          "CREATE TABLE t(id INT PRIMARY KEY NOT NULL, s VARCHAR(128), d DOUBLE)");
        Group g = ais.getGroup(new TableName(SCHEMA, "t"));
        RowDef tRowDef = g.getRoot().rowDef();
        ProtobufRowDataConverter converter = converter(g);
        encodeDecode(ais, converter, tRowDef,
                     1L, "Fred", 3.14);
    }

    @Test
    public void testGroup() throws Exception {
        AkibanInformationSchema ais = ais(
          "CREATE TABLE c(cid INT PRIMARY KEY NOT NULL, name VARCHAR(128));" +
          "CREATE TABLE o(oid INT PRIMARY KEY NOT NULL, cid INT, GROUPING FOREIGN KEY(cid) REFERENCES c(cid));" +
          "CREATE TABLE i(iid INT PRIMARY KEY NOT NULL, oid INT, GROUPING FOREIGN KEY(oid) REFERENCES o(oid), sku VARCHAR(16));");
        Group coi = ais.getGroup(new TableName(SCHEMA, "c"));
        RowDef cRowDef = ais.getTable(new TableName(SCHEMA, "c")).rowDef();
        RowDef oRowDef = ais.getTable(new TableName(SCHEMA, "o")).rowDef();
        RowDef iRowDef = ais.getTable(new TableName(SCHEMA, "i")).rowDef();
        ProtobufRowDataConverter converter = converter(coi);
        encodeDecode(ais, converter, cRowDef,
                     1L, "Fred");
        encodeDecode(ais, converter, oRowDef,
                     101L, 1L);
        encodeDecode(ais, converter, iRowDef,
                     10101L, 101L, "P100");
    }

    @Test
    public void testDecimal() throws Exception {
        AkibanInformationSchema ais = ais(
          "CREATE TABLE t(id INT PRIMARY KEY NOT NULL, n1 DECIMAL(6,2), n2 DECIMAL(20,10))");
        Group g = ais.getGroup(new TableName(SCHEMA, "t"));
        RowDef tRowDef = g.getRoot().rowDef();
        ProtobufRowDataConverter converter = converter(g);
        encodeDecode(ais, converter, tRowDef,
                     1L, new BigDecimal("3.14"), new BigDecimal("1234567890.0987654321"));
    }

    @Test
    public void testNulls() throws Exception {
        AkibanInformationSchema ais = ais(
          "CREATE TABLE t(id INT PRIMARY KEY NOT NULL, s VARCHAR(128) DEFAULT 'abc')");
        Group g = ais.getGroup(new TableName(SCHEMA, "t"));
        RowDef tRowDef = g.getRoot().rowDef();
        ProtobufRowDataConverter converter = converter(g);
        encodeDecode(ais, converter, tRowDef,
                     1L, "Barney");
        encodeDecode(ais, converter, tRowDef,
                     2L, null);
    }

}
