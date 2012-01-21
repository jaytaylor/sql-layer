/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.pg;

import com.akiban.server.types.AkType;

public class PostgresBoundQueryContext extends PostgresQueryContext 
{
    private PostgresStatement statement;
    private boolean[] columnBinary;
    private boolean defaultColumnBinary;
    
    public PostgresBoundQueryContext(PostgresServerSession server, 
                                     PostgresStatement statement,
                                     Object[] parameters,
                                     boolean[] columnBinary, 
                                     boolean defaultColumnBinary) {
        super(server);
        this.statement = statement;
        this.columnBinary = columnBinary;
        this.defaultColumnBinary = defaultColumnBinary;
        if (parameters != null)
            decodeParameters(parameters);
    }

    public PostgresStatement getStatement() {
        return statement;
    }

    public boolean isColumnBinary(int i) {
        if ((columnBinary != null) && (i < columnBinary.length))
            return columnBinary[i];
        else
            return defaultColumnBinary;
    }

    protected void decodeParameters(Object[] parameters) {
        PostgresType[] parameterTypes = statement.getParameterTypes();
        for (int i = 0; i < parameters.length; i++) {
            PostgresType pgType = (parameterTypes == null) ? null : parameterTypes[i];
            AkType akType = null;
            if (pgType != null)
                akType = pgType.getAkType();
            if (akType == null)
                akType = AkType.VARCHAR;
            setValue(i, parameters[i], akType);
        }
    }

}
