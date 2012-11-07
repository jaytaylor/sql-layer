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

package com.akiban.sql.pg;

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

    public PostgresJsonStatement() {
    }

    public void init(Operator resultOperator, RowType resultRowType,
                     List<JsonResultColumn> resultColumns,
                     PostgresType[] parameterTypes,
                     boolean usePValues) {
        super.init(resultOperator, resultRowType,
                   // Looks like just one unlimited VARCHAR to the client.
                   jsonColumnNames(), jsonColumnTypes(),
                   parameterTypes, usePValues);
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
