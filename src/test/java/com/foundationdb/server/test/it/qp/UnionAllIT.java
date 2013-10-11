package com.foundationdb.server.test.it.qp;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.groupScan_Default;
import static com.foundationdb.qp.operator.API.select_HKeyOrdered;
import static com.foundationdb.qp.operator.API.unionAll_Default;
import static com.foundationdb.qp.operator.API.union_Ordered;

import org.junit.Test;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.RowBase;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.UserTableRowType;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.test.ExpressionGenerators;
import com.foundationdb.server.types.texpressions.Comparison;

public class UnionAllIT extends OperatorITBase {

    @Override
    protected void setupCreateSchema()
    {
        t = createTable(
            "schema", "t",
            "id int not null",
            "x int",
            "primary key(id)");
        createIndex("schema", "t", "tx", "x");
        u = createTable (
                "schema", "u",
                "id int not null primary key",
                "x int");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        schema = new Schema(ais());
        tRowType = schema.userTableRowType(userTable(t));
        uRowType = schema.userTableRowType(userTable(u));
        tGroupTable = group(t);
        uGroupTable = group(u);
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        db = new NewRow[]{
            createNewRow(t, 1000L, 8L),
            createNewRow(t, 1001L, 9L),
            createNewRow(t, 1002L, 8L),
            createNewRow(t, 1003L, 9L),
            createNewRow(t, 1004L, 8L),
            createNewRow(t, 1005L, 9L),
            createNewRow(t, 1006L, 8L),
            createNewRow(t, 1007L, 9L),
            createNewRow(u, 1000L, 7L),
            createNewRow(u, 1001L, 9L),
            createNewRow(u, 1002L, 9L),
            createNewRow(u, 1003L, 9L)
        };
        use(db);
    }
    private int t, u;
    private UserTableRowType tRowType, uRowType;
    private Group tGroupTable, uGroupTable;
    
    @Test
    public void testBothNonEmpty()
    {
        Operator plan =
            unionAll_Default(
                select_HKeyOrdered(
                    groupScan_Default(tGroupTable),
                    tRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(tRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(8), castResolver())),
                tRowType,
                select_HKeyOrdered(
                    groupScan_Default(uGroupTable),
                    uRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(uRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(7), castResolver())),
                uRowType, 
                true);
        RowBase[] expected = new RowBase[]{
            row(tRowType, 1000L, 8L),
            row(tRowType, 1002L, 8L),
            row(tRowType, 1004L, 8L),
            row(tRowType, 1006L, 8L),
            row(uRowType, 1000L, 7L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }
    
    @Test
    public void testOrderedNonEmpty()
    {
        Operator plan =
            union_Ordered(
                select_HKeyOrdered(
                    groupScan_Default(tGroupTable),
                    tRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(tRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(9), castResolver())),
                select_HKeyOrdered(
                    groupScan_Default(uGroupTable),
                    uRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(uRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(9), castResolver())),
                tRowType,
                uRowType, 
                2,
                2,
                ascending (true, true),
                false);
        RowBase[] expected = new RowBase[]{
            row(tRowType, 1001L, 9L),
            row(uRowType, 1002L, 9L),
            row(tRowType, 1003L, 9L),
            row(tRowType, 1005L, 9L),
            row(tRowType, 1007L, 9L)
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }
    
    private boolean[] ascending(boolean... ascending)
    {
        return ascending;
    }
    
}
