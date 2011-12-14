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

import java.io.IOException;
import java.util.List;

/** SQL statement to explain another one. */
public class PostgresExplainStatement implements PostgresStatement
{
    private List<String> explanation;
    private String colName;
    private PostgresType colType;
    
    public PostgresExplainStatement(List<String> explanation) {
        this.explanation = explanation;

        int maxlen = 32;
        for (String row : explanation) {
            if (maxlen < row.length())
                maxlen = row.length();
        }
        colName = "OPERATORS";
        colType = new PostgresType(PostgresType.VARCHAR_TYPE_OID, (short)-1, maxlen);
    }

    @Override
    public PostgresStatement getBoundStatement(String[] parameters,
                                               boolean[] columnBinary, 
                                               boolean defaultColumnBinary)  {
        return this;
    }

    @Override
    public void sendDescription(PostgresServerSession server, boolean always) 
            throws IOException {
        PostgresMessenger messenger = server.getMessenger();
        messenger.beginMessage(PostgresMessages.ROW_DESCRIPTION_TYPE.code());
        messenger.writeShort(1);
        messenger.writeString(colName); // attname
        messenger.writeInt(0);    // attrelid
        messenger.writeShort(0);  // attnum
        messenger.writeInt(colType.getOid()); // atttypid
        messenger.writeShort(colType.getLength()); // attlen
        messenger.writeInt(colType.getModifier()); // atttypmod
        messenger.writeShort(0);
        messenger.sendMessage();
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.ALLOWED;
    }

    @Override
    public int execute(PostgresServerSession server, int maxrows)
            throws IOException {
        PostgresMessenger messenger = server.getMessenger();
        int nrows = 0;
        for (String row : explanation) {
            messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
            messenger.writeShort(1);
            byte[] value = colType.encodeValue(row, messenger.getEncoding(), false);
            messenger.writeInt(value.length);
            messenger.write(value);
            messenger.sendMessage();
            nrows++;
            if ((maxrows > 0) && (nrows >= maxrows))
                break;
        }
        {        
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            messenger.writeString("EXPLAIN " + nrows);
            messenger.sendMessage();
        }
        return nrows;
    }

}
