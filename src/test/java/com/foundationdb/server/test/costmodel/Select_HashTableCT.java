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

package com.foundationdb.server.test.costmodel;

import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.TimeOperator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.test.ExpressionGenerators;
import com.foundationdb.server.types.texpressions.TPreparedBoundField;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedField;
import org.junit.Test;

import java.util.*;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.qp.operator.API.groupScan_Default;
import static com.foundationdb.qp.operator.API.project_Default;



public class Select_HashTableCT extends CostModelBase
{
    static int ROW_BINDING_POSITION = 100;
    static int TABLE_BINDING_POSITION = 200;

    @Test
    public void run() throws Exception
    {
        createSchema();
        populateDB();
        drivingRowTypes = new TableRowType[] { d1RowType,d2RowType,d3RowType,d4RowType,d5RowType};
        hashedRowTypes = new TableRowType[] { h1RowType,h2RowType,h3RowType,h4RowType,h5RowType};
        drivingTableIDs = new int[] {d1,d2,d3,d4,d5};
        hashedTableIDs = new int[] {h1,h2,h3,h4,h5};
        // Load-only
        run(WARMUP_RUNS, true, 0, false, 2,1);
        for(int columns = 2; columns <= 6; columns++) {
            for (int joinOn = 1; joinOn <= columns - 1; joinOn++) {
                run(MEASURED_RUNS, true, 0, true, columns, joinOn);
            }
        }
        // Load and scan. Vary the starting value to control the amount of overlap with the filter rows.
        run(WARMUP_RUNS, false, 0, false, 2, 1);
        for(int columns = 2; columns <= 6; columns++)
            for(int joinOn = 1; joinOn <= columns - 1; joinOn++)
                run(MEASURED_RUNS, false, 0, true, columns, joinOn);
    }

    private void createSchema() throws InvalidOperationException
    {
        // Schema is similar to that in Select_BloomHashTableIT
        String schemaName = schemaName();
        String d1TableName = newTableName();
        String d2TableName = newTableName();
        String d3TableName = newTableName();
        String d4TableName = newTableName();
        String d5TableName = newTableName();
        String h1TableName = newTableName();
        String h2TableName = newTableName();
        String h3TableName = newTableName();
        String h4TableName = newTableName();
        String h5TableName = newTableName();

        d1 = createTable(
                schemaName, d1TableName,
                "x int", "y int");
        h1 = createTable(
                schemaName, h1TableName,
                "x int", "y int");
        d2 = createTable(
                schemaName, d2TableName,
                "x int", "y int", "z int");
        h2 = createTable(
                schemaName, h2TableName,
                "x int", "y int", "z int");
        d3 = createTable(
                schemaName, d3TableName,
                "x int", "y int", "z int", "a int");
        h3 = createTable(
                schemaName, h3TableName,
                "x int", "y int", "z int", "a int");
        d4 = createTable(
                schemaName, d4TableName,
                "x int", "y int", "z int", "a int", "b int");
        h4 = createTable(
                schemaName, h4TableName,
                "x int", "y int", "z int", "a int", "b int");
        d5 = createTable(
                schemaName, d5TableName,
                "x int", "y int", "z int", "a int", "b int", "c int");
        h5 = createTable(
                schemaName, h5TableName,
                "x int", "y int", "z int", "a int", "b int", "c int");
        Index d1x = createIndex(schemaName, d1TableName, "idx_dx", "x", "y");
        Index d2x = createIndex(schemaName, d2TableName, "idx_dx", "x", "y", "z");
        Index d3x = createIndex(schemaName, d3TableName, "idx_dx", "x", "y", "z", "a");
        Index d4x = createIndex(schemaName, d4TableName, "idx_dx", "x", "y", "z", "a", "b");
        Index d5x = createIndex(schemaName, d5TableName, "idx_dx", "x", "y", "z", "a", "b", "c");
        Index h1x = createIndex(schemaName, h1TableName, "idx_dx", "x", "y");
        Index h2x = createIndex(schemaName, h2TableName, "idx_dx", "x", "y", "z");
        Index h3x = createIndex(schemaName, h3TableName, "idx_dx", "x", "y", "z", "a");
        Index h4x = createIndex(schemaName, h4TableName, "idx_dx", "x", "y", "z", "a", "b");
        Index h5x = createIndex(schemaName, h5TableName, "idx_dx", "x", "y", "z", "a", "b", "c");

        schema = new Schema(ais());
        d1RowType = schema.tableRowType(table(d1));
        h1RowType = schema.tableRowType(table(h1));
        d2RowType = schema.tableRowType(table(d2));
        h2RowType = schema.tableRowType(table(h2));
        d3RowType = schema.tableRowType(table(d3));
        h3RowType = schema.tableRowType(table(h3));
        d4RowType = schema.tableRowType(table(d4));
        h4RowType = schema.tableRowType(table(h4));
        d5RowType = schema.tableRowType(table(d5));
        h5RowType = schema.tableRowType(table(h5));



        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }
/** x+=2 gives it a 50% match rate **/
    protected void populateDB()
    {
        for (int x = 0; x < HASHED_ROWS; x++) {
            dml().writeRow(session(), createNewRow(h1, x, x, x+1)); // x, hidden_pk
        }
        for (int x = 0; x < DRIVING_ROWS; x +=2) {
            dml().writeRow(session(), createNewRow(d1, x, x, x+1)); // x, hidden_pk
        }
        for (int x = 0; x < HASHED_ROWS; x++) {
            dml().writeRow(session(), createNewRow(h2, x, x, x+1, x+2)); // x, hidden_pk
        }
        for (int x = 0; x < DRIVING_ROWS; x +=2) {
            dml().writeRow(session(), createNewRow(d2, x, x, x+1, x+2)); // x, hidden_pk
        }
        for (int x = 0; x < HASHED_ROWS; x++) {
            dml().writeRow(session(), createNewRow(h3, x, x, x+1, x+2, x+3)); // x, hidden_pk
        }
        for (int x = 0; x < DRIVING_ROWS; x +=2) {
            dml().writeRow(session(), createNewRow(d3, x, x, x+1, x+2, x+3)); // x, hidden_pk
        }
        for (int x = 0; x < HASHED_ROWS; x++) {
            dml().writeRow(session(), createNewRow(h4, x, x, x+1, x+2, x+3, x+4)); // x, hidden_pk
        }
        for (int x = 0; x < DRIVING_ROWS; x +=2) {
            dml().writeRow(session(), createNewRow(d4, x, x, x+1, x+2, x+3, x+4)); // x, hidden_pk
        }
        for (int x = 0; x < HASHED_ROWS; x++) {
            dml().writeRow(session(), createNewRow(h5, x, x, x+1, x+2, x+3, x+4, x+5)); // x, hidden_pk
        }
        for (int x = 0; x < DRIVING_ROWS; x +=2) {
            dml().writeRow(session(), createNewRow(d5, x, x, x+1, x+2, x+3, x+4, x+5)); // x, hidden_pk
        }

    }

    private void run(int runs, boolean loadOnly, int startScan, boolean report, int columnCount, int joinColumns)
    {
        Operator plan = loadOnly ? planLoadOnly(columnCount, joinColumns) : planLoadAndSelect(startScan);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            Row row;
            while ((row = cursor.next()) != null) {
                // System.out.println(row);
            }
        }
        long stop = System.nanoTime();
        long planNsec = stop - start;
        if (loadOnly) {
            planNsec -= timeHashTableInput.elapsedNsec();
            if (report) {
                double averageUsecPerRow = planNsec / (1000.0 * runs * HASHED_ROWS);
                System.out.println(String.format("load only: joined on %d out of %d columns: %s usec/row", joinColumns, columnCount, averageUsecPerRow));
            }
        } else {
            planNsec -= (timeHashTableInput.elapsedNsec() + timeScanInput.elapsedNsec());
            if (report) {
                double averageUsecPerRow = planNsec / (1000.0 * runs * (DRIVING_ROWS - startScan));
                double selected = (double) (HASHED_ROWS - startScan) / DRIVING_ROWS;
                System.out.println(String.format("scan %s: %s usec/row", selected, averageUsecPerRow));
            }
        }
    }

    public Operator planLoadOnly(int columnCount, int joinColumns)
    {
        RowType innerRowType = hashedRowTypes[columnCount -2];
        RowType outerRowType = drivingRowTypes[columnCount -2];
        // filterInput loads the filter with F rows containing the given testId.
        timeHashTableInput = new TimeOperator(groupScan_Default(group(hashedTableIDs[columnCount -2]))
        );

        Operator outerStream = groupScan_Default(group(drivingTableIDs[columnCount -2]));
        // For the  index scan retrieving rows from the F(x) index given a D index row

        // Use a bloom filter loaded by filterInput. Then for each input row, check the filter (projecting
        // D rows on (x)), and, for positives, check F using an index scan keyed by D.x.
        int joinFields[] = new int[joinColumns];
        for(int i = 0;  i <joinColumns; i++){
            joinFields[i] = i;
    }

        return hashJoinPlan(outerRowType, innerRowType,outerStream,timeHashTableInput, joinFields, joinFields, null, true);
    }

    private Operator hashJoinPlan( RowType outerRowType,
                                   RowType innerRowType,
                                   Operator outerStream,
                                   Operator innerStream,
                                   int outerJoinFields[],
                                   int innerJoinFields[],
                                   List<AkCollator> collators,
                                   Boolean loadOnly) {

        List<TPreparedExpression> expressions = new ArrayList<>();
        for( int i = 0; i < outerRowType.nFields(); i++){
            expressions.add(new TPreparedBoundField(outerRowType, ROW_BINDING_POSITION, i));
        }
        for( int i = 0, j = 0; i < innerRowType.nFields(); i++){
            if(j < innerJoinFields.length && innerJoinFields[j] == i) {
                j++;
            }else{
                expressions.add(new TPreparedField(innerRowType.typeAt(i), i));
            }
        }

        List<TPreparedExpression> outerJoinExpressions = new ArrayList<>();
        for(int i : outerJoinFields){
            outerJoinExpressions.add(new TPreparedField(outerRowType.typeAt(i), i));
        }
        List<TPreparedExpression> innerJoinExpressions = new ArrayList<>();
        for(int i : innerJoinFields){
            innerJoinExpressions.add(new TPreparedField(innerRowType.typeAt(i), i));
        }

        Operator project = project_Default(
                hashTableLookup_Default(
                        collators,
                        outerJoinExpressions,
                        TABLE_BINDING_POSITION
                ),
                innerRowType,
                expressions
        );
        return using_HashTable(
                innerStream,
                innerRowType,
                innerJoinExpressions,
                TABLE_BINDING_POSITION++,
                map_NestedLoops(
                        outerStream,
                        project,
                        ROW_BINDING_POSITION++,
                        false,
                        1
                ),
                collators
        );
    }

    public Operator planLoadAndSelect(int start)
    { return null;}
     /*   // filterInput loads the filter with F rows containing the given testId.
        Operator filterInput =
                project_DefaultTest(
                        groupScan_Default(group(f)),
                        fRowType,
                        Arrays.asList(ExpressionGenerators.field(fRowType, 0)));
        timeHashTableInput = new TimeOperator(filterInput);
        // For the index scan retriving rows from the D(x) index
        IndexBound dxLo =
                new IndexBound(row(dIndexRowType, start), new SetColumnSelector(0));
        IndexBound dxHi =
                new IndexBound(row(dIndexRowType, Integer.MAX_VALUE), new SetColumnSelector(0));
        IndexKeyRange dKeyRange =
                IndexKeyRange.bounded(dIndexRowType, dxLo, true, dxHi, false);
        // For the  index scan retrieving rows from the F(x) index given a D index row
        IndexBound fxBound = new IndexBound(
                new RowBasedUnboundExpressions(
                        filterInput.rowType(),
                        Arrays.asList(ExpressionGenerators.boundField(dIndexRowType, 0, 0)), true),
                new SetColumnSelector(0));
        IndexKeyRange fKeyRange = IndexKeyRange.bounded(fIndexRowType, fxBound, true, fxBound, true);
        // Use a bloom filter loaded by filterInput. Then for each input row, check the filter (projecting
        // D rows on (x)), and, for positives, check F using an index scan keyed by D.x.
        Operator scanInput = indexScan_Default(dIndexRowType, dKeyRange, new Ordering());
        timeScanInput = new TimeOperator(scanInput);
        Operator plan =
                using_BloomFilter(
                        // filterInput
                        timeHashTableInput,
                        // filterRowType
                        filterInput.rowType(),
                        // estimatedRowCount
                        HASHED_ROWS,
                        // filterBindingPosition
                        0,
                        // streamInput
                        select_BloomFilterTest(
                                // input
                                timeScanInput,
                                // onPositive
                                indexScan_Default(
                                        fIndexRowType,
                                        fKeyRange,
                                        new Ordering()),
                                // filterFields
                                Arrays.asList(ExpressionGenerators.field(dIndexRowType, 0)),
                                // filterBindingPosition, pipeline, depth
                                0, false, 1));
        return plan;
    }*/

    private static final int WARMUP_RUNS = 2;
    private static final int MEASURED_RUNS = 5;
    private static final long HASHED_ROWS = 10;
    private static final long DRIVING_ROWS = 20;

    private int d1, d2, d3, d4, d5;
    private int h1, h2, h3, h4, h5;
    
    private TableRowType d1RowType;
    private TableRowType d2RowType;
    private TableRowType d3RowType;
    private TableRowType d4RowType;
    private TableRowType d5RowType;
    private TableRowType h1RowType;
    private TableRowType h2RowType;
    private TableRowType h3RowType;
    private TableRowType h4RowType;
    private TableRowType h5RowType;

    int drivingTableIDs[];
    int hashedTableIDs[];
    TableRowType drivingRowTypes[];
    TableRowType hashedRowTypes[];

    private IndexRowType dIndexRowType;
    private IndexRowType fIndexRowType;
    private TimeOperator timeHashTableInput;
    private TimeOperator timeScanInput;
}
