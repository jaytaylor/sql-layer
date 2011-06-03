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

import com.akiban.sql.StandardException;

import java.util.*;
import java.io.IOException;

/**
 * An ordinary SQL statement.
 */
public abstract class PostgresBaseStatement implements PostgresStatement
{
    private List<String> columnNames;
    private List<PostgresType> columnTypes;

    protected PostgresBaseStatement() {
    }

    protected PostgresBaseStatement(List<String> columnNames, 
                                    List<PostgresType> columnTypes) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<PostgresType> getColumnTypes() throws StandardException {
        return columnTypes;
    }

    public boolean isColumnBinary(int i) {
        return false;
    }

    public void sendDescription(PostgresServerSession server, boolean always) 
            throws IOException, StandardException {
        PostgresMessenger messenger = server.getMessenger();
        List<PostgresType> columnTypes = getColumnTypes();
        if (columnTypes == null) {
            if (!always) return;
            messenger.beginMessage(PostgresMessenger.NO_DATA_TYPE);
        }
        else {
            messenger.beginMessage(PostgresMessenger.ROW_DESCRIPTION_TYPE);
            List<String> columnNames = getColumnNames();
            int ncols = columnTypes.size();
            messenger.writeShort(ncols);
            for (int i = 0; i < ncols; i++) {
                PostgresType type = columnTypes.get(i);
                messenger.writeString(columnNames.get(i)); // attname
                messenger.writeInt(0);    // attrelid
                messenger.writeShort(0);  // attnum
                messenger.writeInt(type.getOid()); // atttypid
                messenger.writeShort(type.getLength()); // attlen
                messenger.writeInt(type.getModifier()); // atttypmod
                messenger.writeShort(isColumnBinary(i) ? 1 : 0);
            }
        }
        messenger.sendMessage();
    }

}
