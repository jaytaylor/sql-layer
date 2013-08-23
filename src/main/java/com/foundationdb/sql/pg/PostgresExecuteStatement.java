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
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.TPreptimeValue;
import com.foundationdb.server.types3.pvalue.PValueSources;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

public class PostgresExecuteStatement extends PostgresBaseCursorStatement
{
    private String name;
    private List<TPreptimeValue> paramPValues; 

    public String getName() {
        return name;
    }

    public void setParameters(QueryBindings bindings) {
        for (int i = 0; i < paramPValues.size(); i++) {
            bindings.setPValue(i, paramPValues.get(i).value());
        }
    }

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        ExecuteStatementNode execute = (ExecuteStatementNode)stmt;
        this.name = execute.getName();
        paramPValues = new ArrayList<>();
        for (ValueNode param : execute.getParameterList()) {
            TInstance tInstance = null;
            if (param.getType() != null)
                tInstance = TypesTranslation.toTInstance(param.getType());
            if (!(param instanceof ConstantNode)) {
                throw new UnsupportedSQLException("EXECUTE arguments must be constants", param);
            }
            ConstantNode constant = (ConstantNode)param;
            Object value = constant.getValue();
            paramPValues.add(PValueSources.fromObject(value, tInstance));
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
