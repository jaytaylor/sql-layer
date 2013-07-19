/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.qp.persistitadapter.indexcursor;

import static com.akiban.qp.operator.API.cursor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.ExpressionGenerator;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryBindings;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.indexcursor.MergeJoinSorter.KeyReadCursor;
import com.akiban.qp.row.BindableRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.test.it.ITBase;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.persistit.Key;

import static com.akiban.qp.operator.API.*;
import static com.akiban.server.test.ExpressionGenerators.literal;
import static org.junit.Assert.*;


public class CursorToKeyWriterIT extends ITBase {
 
    @Before
    public void testSetup() {
        schema = new Schema(ais());
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }
    
    @Test
    public void testSimple1() throws IOException {
        RowType innerValuesRowType = schema.newValuesType(MString.varchar());
        List<BindableRow> innerValuesRows = new ArrayList<>();
        innerValuesRows.add(BindableRow.of(innerValuesRowType, Collections.singletonList(literal(null)), null));
        
        KeyReadCursor keyCursor = getKeyCursor(innerValuesRowType, innerValuesRows);
        
        Key key = keyCursor.readNext();
        assertTrue (keyCursor.rowCount() == 1);
        assertNotNull (key);
        assertTrue(key.getEncodedSize() == 2);
        assertTrue(key.isNull());
        
        key = keyCursor.readNext();
        assertNull (key);
    }

    @Test 
    public void testSimpleInt() throws IOException{
        RowType innerValuesRowType = schema.newValuesType(MNumeric.INT.instance(true));
        List<BindableRow> innerValuesRows = new ArrayList<>();
        innerValuesRows.add(BindableRow.of(innerValuesRowType, Collections.singletonList(literal(1)), queryContext));
        
        KeyReadCursor keyCursor = getKeyCursor(innerValuesRowType, innerValuesRows);
        
        Key key = keyCursor.readNext();
        assertNotNull (key);
        assertTrue (!key.isNull());
        assertTrue (key.decodeType() == Long.class);
        assertTrue (key.decodeLong() == 1L);
        
        key = keyCursor.readNext();
        assertNull (key);
    }
    
    @Test 
    public void testSimpleString() throws IOException {
        RowType innerValuesRowType = schema.newValuesType(MString.varchar());
        List<BindableRow> innerValuesRows = new ArrayList<>();
        innerValuesRows.add(BindableRow.of(innerValuesRowType, Collections.singletonList(literal("a")), queryContext));

        KeyReadCursor keyCursor = getKeyCursor(innerValuesRowType, innerValuesRows);
        Key key = keyCursor.readNext();
        assertNotNull (key);
        assertTrue (!key.isNull());
        assertTrue (key.decodeType() == String.class);
        assertTrue (key.decodeString().equals("a"));
        
        key = keyCursor.readNext();
        assertNull (key);      
    }
    
    @Test
    public void testThreeValues() throws IOException {
        RowType rowType = schema.newValuesType(MNumeric.INT.instance(false),MNumeric.INT.instance(true), MString.varchar());
        List<BindableRow> rows = new ArrayList<>();
        List<ExpressionGenerator> values = new ArrayList<>();
        values.add(literal(1));
        values.add(literal(1000));
        values.add(literal("A"));
        rows.add(BindableRow.of(rowType,  values, queryContext));
        KeyReadCursor keyCursor = getKeyCursor(rowType , rows);
        Key key = keyCursor.readNext();
        assertNotNull (key);
        assertTrue (!key.isNull());
        assertTrue(key.decodeLong() == 1);
        assertTrue(key.decodeLong() == 1000);
        assertTrue(key.decodeString().equals("A"));
        
        key = keyCursor.readNext();
        assertNull (key);      
        
    }
    
    
    private KeyReadCursor getKeyCursor (RowType rowType, List<BindableRow> rows) {
        Operator op = valuesScan_Default(rows, rowType);
        Cursor cursor = cursor(op, queryContext, queryBindings);
        cursor.open();
        return new KeyReadCursor (queryContext, cursor, rowType);
    }
    
    protected Schema schema;
    protected StoreAdapter adapter;
    protected QueryBindings queryBindings;
    protected QueryContext queryContext;

}
