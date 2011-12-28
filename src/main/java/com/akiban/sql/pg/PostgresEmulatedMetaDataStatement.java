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

import com.akiban.server.error.UnsupportedParametersException;
import com.akiban.server.types.AkType;

import java.io.IOException;

/**
 * Canned handling for fixed SQL text that comes from tools that
 * believe they are talking to a real Postgres database.
 */
public class PostgresEmulatedMetaDataStatement implements PostgresStatement
{
    enum Query {
        // ODBC driver sends this at the start; returning no rows is fine (and normal).
        ODBC_LO_TYPE_QUERY("select oid, typbasetype from pg_type where typname = 'lo'");

        // TODO: May need regex for some cases.
        private String sql;

        Query(String sql) {
            this.sql = sql;
        }

        public String getSQL() {
            return sql;
        }
    }

    private Query query;

    protected PostgresEmulatedMetaDataStatement(Query query) {
        this.query = query;
    }

    @Override
    public PostgresStatement getBoundStatement(String[] parameters,
                                               boolean[] columnBinary, 
                                               boolean defaultColumnBinary)  {
        if (parameters != null)
            throw new UnsupportedParametersException();
        return this;
    }

    static final PostgresType OID_PG_TYPE = 
        new PostgresType(PostgresType.OID_TYPE_OID, (short)4, -1, AkType.LONG);

    @Override
    public void sendDescription(PostgresServerSession server, boolean always) 
            throws IOException {
        int ncols;
        String[] names;
        PostgresType[] types;
        switch (query) {
        case ODBC_LO_TYPE_QUERY:
            ncols = 2;
            names = new String[] { "oid", "typbasetype" };
            types = new PostgresType[] { OID_PG_TYPE, OID_PG_TYPE };
            break;
        default:
            return;
        }

        PostgresMessenger messenger = server.getMessenger();
        messenger.beginMessage(PostgresMessages.ROW_DESCRIPTION_TYPE.code());
        messenger.writeShort(ncols);
        for (int i = 0; i < ncols; i++) {
            PostgresType type = types[i];
            messenger.writeString(names[i]); // attname
            messenger.writeInt(0);    // attrelid
            messenger.writeShort(0);  // attnum
            messenger.writeInt(type.getOid()); // atttypid
            messenger.writeShort(type.getLength()); // attlen
            messenger.writeInt(type.getModifier()); // atttypmod
            messenger.writeShort(0);
        }
        messenger.sendMessage();
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.READ;
    }

    @Override
    public int execute(PostgresServerSession server, int maxrows)
        throws IOException {
        PostgresMessenger messenger = server.getMessenger();
        int nrows = 0;
        switch (query) {
        case ODBC_LO_TYPE_QUERY:
            nrows = odbcLoTypeQuery(messenger, maxrows);
            break;
        }
        {        
          messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
          messenger.writeString("SELECT " + nrows);
          messenger.sendMessage();
        }
        return nrows;
    }

    private int odbcLoTypeQuery(PostgresMessenger messenger, int maxrows) {
        return 0;
    }

}
