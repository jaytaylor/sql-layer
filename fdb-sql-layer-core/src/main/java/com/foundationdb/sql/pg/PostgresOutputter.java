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

import com.foundationdb.sql.server.ServerValueEncoder;

import java.util.List;
import java.io.IOException;

public abstract class PostgresOutputter<T>
{
    protected PostgresMessenger messenger;
    protected PostgresQueryContext context;
    protected PostgresDMLStatement statement;
    protected List<PostgresType> columnTypes;
    protected int ncols;
    protected ServerValueEncoder encoder;

    public PostgresOutputter(PostgresQueryContext context,
                             PostgresDMLStatement statement) {
        this.context = context;
        this.statement = statement;
        PostgresServerSession server = context.getServer();
        messenger = server.getMessenger();
        columnTypes = statement.getColumnTypes();
        if (columnTypes != null)
            ncols = columnTypes.size();
        encoder = server.getValueEncoder();
    }

    public void beforeData() throws IOException {}

    public void afterData() throws IOException {}

    public abstract void output(T row) throws IOException;
}
