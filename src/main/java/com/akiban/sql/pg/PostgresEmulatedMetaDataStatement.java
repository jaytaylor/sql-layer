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

import java.io.IOException;

/**
 * Canned handling for fixed SQL text that comes from tools that
 * believe they are talking to a real Postgres database.
 */
public class PostgresEmulatedMetaDataStatement extends PostgresStatement
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

    public void execute(PostgresServerSession server, int maxrows)
        throws IOException, StandardException {
        PostgresMessenger messenger = server.getMessenger();
        int nrows = 0;
        switch (query) {
        case ODBC_LO_TYPE_QUERY:
            nrows = odbcLoTypeQuery(messenger, maxrows);
            break;
        }
        {        
          messenger.beginMessage(PostgresMessenger.COMMAND_COMPLETE_TYPE);
          messenger.writeString("SELECT " + nrows);
          messenger.sendMessage();
        }
    }

    private int odbcLoTypeQuery(PostgresMessenger messenger, int maxrows) {
        return 0;
    }

}
