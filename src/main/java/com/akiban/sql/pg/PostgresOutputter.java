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

import com.akiban.sql.server.ServerValueEncoder;

import java.util.List;
import java.io.IOException;

public abstract class PostgresOutputter<T>
{
    protected PostgresMessenger messenger;
    protected PostgresBaseStatement statement;
    protected List<PostgresType> columnTypes;
    protected int ncols;
    protected ServerValueEncoder encoder;

    public PostgresOutputter(PostgresMessenger messenger, 
                             PostgresBaseStatement statement) {
        this.messenger = messenger;
        this.statement = statement;
        columnTypes = statement.getColumnTypes();
        ncols = columnTypes.size();
        encoder = new ServerValueEncoder(messenger.getEncoding());
    }

    public abstract void output(T row) throws IOException;
}
