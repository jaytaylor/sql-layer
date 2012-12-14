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

import com.akiban.sql.optimizer.TypesTranslation;
import com.akiban.sql.parser.ConstantNode;
import com.akiban.sql.parser.ExecuteStatementNode;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.ValueNode;

import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.pvalue.PValueSources;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

public class PostgresExecuteStatement extends PostgresBaseCursorStatement
{
    private String name;
    private List<ValueSource> paramValues;
    private List<TPreptimeValue> paramPValues; 

    public String getName() {
        return name;
    }

    public void setParameters(PostgresBoundQueryContext context) {
        if (paramPValues != null) {
            for (int i = 0; i < paramPValues.size(); i++) {
                context.setPValue(i, paramPValues.get(i).value());
            }
        }
        else {
            for (int i = 0; i < paramValues.size(); i++) {
                context.setValue(i, paramValues.get(i));
            }
        }
    }

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        ExecuteStatementNode execute = (ExecuteStatementNode)stmt;
        this.name = execute.getName();
        FromObjectValueSource fromObject = null;
        if (Types3Switch.ON) {
            paramPValues = new ArrayList<TPreptimeValue>();
        }
        else {
            paramValues = new ArrayList<ValueSource>();
            fromObject = new FromObjectValueSource();
        }
        for (ValueNode param : execute.getParameterList()) {
            AkType akType = null;
            if (param.getType() != null)
                akType = TypesTranslation.sqlTypeToAkType(param.getType());
            if (!(param instanceof ConstantNode)) {
                throw new UnsupportedSQLException("EXECUTE arguments must be constants", param);
            }
            ConstantNode constant = (ConstantNode)param;
            Object value = constant.getValue();
            if (paramPValues != null)
                paramPValues.add(PValueSources.fromObject(value, akType));
            else
                paramValues.add(new ValueHolder(fromObject.setExplicitly(value, akType)));
        }
        return this;
    }

    @Override
    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        return server.executePreparedStatement(this, maxrows);
    }
    
    public boolean putInCache() {
        return true;
    }

}
