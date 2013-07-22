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
import static com.akiban.qp.operator.API.valuesScan_Default;
import static com.akiban.server.test.ExpressionGenerators.literal;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.ExpressionGenerator;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.RowCursor;
import com.akiban.qp.persistitadapter.indexcursor.MergeJoinSorter.KeyReadCursor;
import com.akiban.qp.persistitadapter.indexcursor.MergeJoinSorter.KeyReader;
import com.akiban.qp.persistitadapter.indexcursor.MergeJoinSorter.KeyWriter;
import com.akiban.qp.persistitadapter.indexcursor.MergeJoinSorter.KeyFinalCursor;
import com.akiban.qp.row.BindableRow;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.test.it.qp.OperatorITBase;
import com.akiban.server.test.it.qp.TestRow;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.persistit.Key;
import com.persistit.Persistit;

public class KeyFinalCursorIT extends OperatorITBase {


    private Schema schema;
    private ByteArrayOutputStream os;
    private ByteArrayInputStream is;
    private Key startKey; 
    private KeyWriter writer;
    private List<BindableRow> bindRows;
    
    @Before
    public void createFileBuffers() {
        schema = new Schema(ais());
        os = new ByteArrayOutputStream();
        startKey = new Key ((Persistit)null);
        startKey.clear();
        writer = new KeyWriter(os);
        bindRows = new ArrayList<>();

    }

    @Test
    public void cycleSimple() throws IOException {
        RowType rowType = schema.newValuesType(MNumeric.INT.instance(true));
        startKey.append(1L);
        writer.writeEntry(startKey);
        verifyInput();

        is = new ByteArrayInputStream (os.toByteArray());
        RowCursor cursor = new KeyFinalCursor(is, rowType);

        RowBase[] expected = new RowBase[]{
            row(rowType, 1L),
        };
        compareRows(expected, cursor);
    }
    
    @Test
    public void cycleComplete() throws IOException {
        RowType rowType = schema.newValuesType(MNumeric.INT.instance(true));
        
        TestRow[] rows = new TestRow[] {
                row(rowType, 1L),
        };
        
        bindRows.add(BindableRow.of(rows[0], true));
    
        RowCursor cursor = cycleRows(rowType);
        compareRows(rows, cursor);
    }

    @Test
    public void cycleNValues() throws IOException {
        RowType rowType = schema.newValuesType(MNumeric.INT.instance(false),MNumeric.INT.instance(true), MString.varchar());
        TestRow[] rows = new TestRow[] {
                row(rowType, 1L, 100L, "A"),
        };
        
        bindRows.add(BindableRow.of(rows[0], true));
        RowCursor cursor = cycleRows (rowType);
        compareRows(rows, cursor);
    }
    
    private RowCursor cycleRows(RowType rowType) throws IOException {
        KeyReadCursor keyCursor = getKeyCursor(rowType , bindRows);

        startKey  = keyCursor.readNext(); 
        while (startKey != null) {
            writer.writeEntry(startKey);
            startKey = keyCursor.readNext();
        }
        
        is = new ByteArrayInputStream (os.toByteArray());
        RowCursor cursor = new KeyFinalCursor(is, rowType);
        return cursor;
        
    }
    
    private KeyReadCursor getKeyCursor (RowType rowType, List<BindableRow> rows) {
        Operator op = valuesScan_Default(rows, rowType);
        Cursor cursor = cursor(op, queryContext, queryBindings);
        cursor.open();
        return new KeyReadCursor (queryContext, cursor, rowType);
    }

    
    private void verifyInput() throws IOException {
        is = new ByteArrayInputStream (os.toByteArray());
        KeyReader reader = new KeyReader (is);
        Key endKey = reader.readNext();
        assertTrue (startKey.compareTo(endKey) == 0);
        
    }
/*    
    RowType rowType = schema.newValuesType(MNumeric.INT.instance(false),MNumeric.INT.instance(true), MString.varchar());
    List<BindableRow> rows = new ArrayList<>();
    List<ExpressionGenerator> values = new ArrayList<>();
    values.add(literal(1));
    values.add(literal(1000));
    values.add(literal("A"));
    rows.add(BindableRow.of(rowType,  values, queryContext));
*/
}
