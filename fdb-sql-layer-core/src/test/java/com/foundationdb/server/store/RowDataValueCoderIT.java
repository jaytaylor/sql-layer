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
package com.foundationdb.server.store;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.SchemaFactory;
import com.foundationdb.server.store.RowValueCoder.SchemaCoderContext;
import com.foundationdb.server.test.it.PersistitITBase;
import com.persistit.DefaultCoderManager;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.Value;
import com.persistit.encoding.CoderManager;
import com.persistit.exception.PersistitException;

public class RowDataValueCoderIT extends PersistitITBase {
    private Persistit persistit;
    private RowValueCoder coder; 
    
    @Before
    public void setup () {
        this.coder = new RowValueCoder();
        CoderManager cm = treeService().getDb().getCoderManager();
        
        if (cm.getValueCoder(ValuesHolderRow.class) == null) {
            cm.registerValueCoder(ValuesHolderRow.class, coder);
        }
    }

    @Test
    public void oneColumn() {
        
        AkibanInformationSchema ais = ais ("CREATE TABLE c(cid INT PRIMARY KEY NOT NULL);");
        
        Value value = new Value (treeService().getDb());
        
        RowType rowType = schema.tableRowType(ais.getTable(SCHEMA, "c"));

        Key key = new Key(treeService().getDb());
        key.append(ais.getTable(SCHEMA, "c").getTableId());
        key.append(4);
        
        ValuesHolderRow  row = new ValuesHolderRow (rowType, new Integer(4));
        
        SchemaCoderContext context = new SchemaCoderContext(schema, ais.getTable(SCHEMA, "c").getGroup(), key);
        
        value.directPut(coder, row, context);
        
        ValuesHolderRow newRow = (ValuesHolderRow)value.directGet(coder, ValuesHolderRow.class, context);
        
        assertEquals(row.compareTo(newRow, 0, 0, 1), 0);
    }
    
    @Test
    public void twoColumns() {
        
        AkibanInformationSchema ais = ais ("CREATE TABLE c(cid INT PRIMARY KEY NOT NULL, iid INT NOT NULL);");
        
        Value value = new Value (treeService().getDb());
        
        RowType rowType = schema.tableRowType(ais.getTable(SCHEMA, "c"));

        Key key = new Key(treeService().getDb());
        key.append(ais.getTable(SCHEMA, "c").getTableId());
        key.append(4);
        
        ValuesHolderRow  row = new ValuesHolderRow (rowType, new Integer(4), new Integer(11));
        
        SchemaCoderContext context = new SchemaCoderContext(schema, ais.getTable(SCHEMA, "c").getGroup(), key);
        
        value.directPut(coder, row, context);
        
        ValuesHolderRow newRow = (ValuesHolderRow)value.directGet(coder, ValuesHolderRow.class, context);
        
        assertEquals(row.compareTo(newRow, 0, 0, 2), 0);
    }

    @Test
    public void twoColumnString() {
        
        AkibanInformationSchema ais = ais ("CREATE TABLE c(cid INT PRIMARY KEY NOT NULL, name VARCHAR(32) NOT NULL);");
        
        Value value = new Value (treeService().getDb());
        
        RowType rowType = schema.tableRowType(ais.getTable(SCHEMA, "c"));

        Key key = new Key(treeService().getDb());
        key.append(ais.getTable(SCHEMA, "c").getTableId());
        key.append(4);
        
        ValuesHolderRow  row = new ValuesHolderRow (rowType, new Integer(4), "Fred Thompson");
        
        SchemaCoderContext context = new SchemaCoderContext(schema, ais.getTable(SCHEMA, "c").getGroup(), key);
        
        value.directPut(coder, row, context);
        
        ValuesHolderRow newRow = (ValuesHolderRow)value.directGet(coder, ValuesHolderRow.class, context);
        
        assertEquals(row.compareTo(newRow, 0, 0, 2), 0);
    }
    
    @Test
    public void twoColumnBigDecimal() {
        
        AkibanInformationSchema ais = ais ("CREATE TABLE c(cid INT PRIMARY KEY NOT NULL, cost NUMERIC NOT NULL);");
        
        Value value = new Value (treeService().getDb());
        
        RowType rowType = schema.tableRowType(ais.getTable(SCHEMA, "c"));

        Key key = new Key(treeService().getDb());
        key.append(ais.getTable(SCHEMA, "c").getTableId());
        key.append(4);
        
        ValuesHolderRow  row = new ValuesHolderRow (rowType, new Integer(4), new BigDecimal(42));
        
        SchemaCoderContext context = new SchemaCoderContext(schema, ais.getTable(SCHEMA, "c").getGroup(), key);
        
        value.directPut(coder, row, context);
        
        ValuesHolderRow newRow = (ValuesHolderRow)value.directGet(coder, ValuesHolderRow.class, context);
        
        assertEquals(row.compareTo(newRow, 0, 0, 2), 0);
    }
    
    @Test
    public void twoColumnsUUID() {
        
        AkibanInformationSchema ais = ais ("CREATE TABLE c(cid INT PRIMARY KEY NOT NULL, identifier GUID NOT NULL);");
        
        Value value = new Value (treeService().getDb());
        
        RowType rowType = schema.tableRowType(ais.getTable(SCHEMA, "c"));

        Key key = new Key(treeService().getDb());
        key.append(ais.getTable(SCHEMA, "c").getTableId());
        key.append(4);
        
        ValuesHolderRow  row = new ValuesHolderRow (rowType, new Integer(4), new UUID(42,51));
        
        SchemaCoderContext context = new SchemaCoderContext(schema, ais.getTable(SCHEMA, "c").getGroup(), key);
        
        value.directPut(coder, row, context);
        
        ValuesHolderRow newRow = (ValuesHolderRow)value.directGet(coder, ValuesHolderRow.class, context);
        
        assertEquals(row.compareTo(newRow, 0, 0, 2), 0);
        
    }
    
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
        schema = new Schema (ais);
        return ais;
    }

    private Schema schema;
    private static final String SCHEMA = "test";

}
