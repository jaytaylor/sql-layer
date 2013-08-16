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

package com.foundationdb.sql.pg;

import com.foundationdb.sql.optimizer.TypesTranslation;
import com.foundationdb.sql.parser.ConstantNode;
import com.foundationdb.sql.parser.ExecuteStatementNode;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.parser.ValueNode;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.FromObjectValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.util.ValueHolder;
import com.foundationdb.server.types3.TPreptimeValue;
import com.foundationdb.server.types3.Types3Switch;
import com.foundationdb.server.types3.pvalue.PValueSources;

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

    public void setParameters(QueryBindings bindings) {
        if (paramPValues != null) {
            for (int i = 0; i < paramPValues.size(); i++) {
                bindings.setPValue(i, paramPValues.get(i).value());
            }
        }
        else {
            for (int i = 0; i < paramValues.size(); i++) {
                bindings.setValue(i, paramValues.get(i));
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
            paramPValues = new ArrayList<>();
        }
        else {
            paramValues = new ArrayList<>();
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
    public void sendDescription(PostgresQueryContext context,
                                boolean always, boolean params)
            throws IOException {
        // Execute will do it.
    }

    @Override
    public int execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        return server.executePreparedStatement(this, maxrows);
    }
    
    @Override
    public boolean putInCache() {
        return true;
    }

}
