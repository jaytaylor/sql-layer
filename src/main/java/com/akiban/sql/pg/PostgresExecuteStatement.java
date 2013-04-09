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
    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        return server.executePreparedStatement(this, maxrows);
    }
    
    @Override
    public boolean putInCache() {
        return true;
    }

}
