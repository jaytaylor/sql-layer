
package com.akiban.sql.pg;

import com.akiban.sql.optimizer.plan.CostEstimate;
import static com.akiban.sql.pg.PostgresJsonCompiler.JsonResultColumn;

import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.mcompat.mtypes.MString;

import java.util.*;

public class PostgresJsonStatement extends PostgresOperatorStatement
{
    private List<JsonResultColumn> resultColumns;

    public PostgresJsonStatement(PostgresOperatorCompiler compiler) {
        super(compiler);
    }

    public void init(Operator resultOperator, RowType resultRowType,
                     List<JsonResultColumn> resultColumns,
                     PostgresType[] parameterTypes,
                     CostEstimate costEstimate,
                     boolean usePValues) {
        super.init(resultOperator, resultRowType,
                   // Looks like just one unlimited VARCHAR to the client.
                   jsonColumnNames(), jsonColumnTypes(),
                   parameterTypes, costEstimate, usePValues);
        this.resultColumns = resultColumns;
    }

    public static List<String> jsonColumnNames() {
        return Collections.singletonList("JSON");
    }

    public static List<PostgresType> jsonColumnTypes() {
        return Collections.singletonList(new PostgresType(PostgresType.TypeOid.VARCHAR_TYPE_OID,
                                                          (short)-1, -1, AkType.VARCHAR,
                                                          MString.VARCHAR.instance(false)));
    }

    @Override
    protected PostgresOutputter<Row> getRowOutputter(PostgresQueryContext context) {
        return new PostgresJsonOutputter(context, this, 
                                         resultColumns, getColumnTypes().get(0));
    }
    
}
