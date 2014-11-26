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

import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.sql.parser.ConstantNode;
import com.foundationdb.sql.parser.ExecuteStatementNode;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.parser.ValueNode;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

public class PostgresExecuteStatement extends PostgresBaseCursorStatement
{
    private String name;
    private List<TPreptimeValue> paramValues; 

    public String getName() {
        return name;
    }

    public void setParameters(QueryBindings bindings) {
        for (int i = 0; i < paramValues.size(); i++) {
            bindings.setValue(i, paramValues.get(i).value());
        }
    }

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        ExecuteStatementNode execute = (ExecuteStatementNode)stmt;
        this.name = execute.getName();
        paramValues = new ArrayList<>();
        for (ValueNode param : execute.getParameterList()) {
            TInstance type;
            if (!(param instanceof ConstantNode)) {
                throw new UnsupportedSQLException("EXECUTE arguments must be constants", param);
            }
            if (param.getType() != null)
                type = server.typesTranslator().typeForSQLType(param.getType());
            else
                type = server.typesTranslator().typeForString();
            ConstantNode constant = (ConstantNode)param;
            Object value = constant.getValue();
            paramValues.add(ValueSources.fromObject(value, type));
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
