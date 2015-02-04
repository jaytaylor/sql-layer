/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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

import com.foundationdb.sql.parser.StatementNode;

import static com.foundationdb.sql.pg.PostgresStatement.PostgresStatementResult;

import java.io.IOException;

public class PostgresStatementResults
{
    public static PostgresStatementResult commandComplete(final String tag,
                                                          final int nrows) {
        return new PostgresStatementResult() {
                @Override
                public void sendCommandComplete(PostgresMessenger messenger) throws IOException {
                    messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
                    messenger.writeString(tag);
                    messenger.sendMessage();
                }
                
                public int getRowsProcessed() {
                    return nrows;
                }
            };
    }

    public static PostgresStatementResult commandComplete(String tag) {
        return commandComplete(tag, 0);
    }

    public static PostgresStatementResult statementComplete(StatementNode stmt, int nrows) {
        return commandComplete(stmt.statementToString(), nrows);
    }

    public static PostgresStatementResult statementComplete(StatementNode stmt) {
        return statementComplete(stmt, 0);
    }

    public static PostgresStatementResult portalSuspended(final int nrows) {
        return new PostgresStatementResult() {
                @Override
                public void sendCommandComplete(PostgresMessenger messenger) throws IOException {
                    messenger.beginMessage(PostgresMessages.PORTAL_SUSPENDED_TYPE.code());
                    messenger.sendMessage();
                }
                
                public int getRowsProcessed() {
                    return nrows;
                }
            };
    }

    // Used for EXECUTE and other recursive statements that have already sent result.
    public static PostgresStatementResult noResult(final int nrows) {
        return new PostgresStatementResult() {
                @Override
                public void sendCommandComplete(PostgresMessenger messenger) {
                }
                
                public int getRowsProcessed() {
                    return nrows;
                }
            };
    }

    // Used when the connection has been shut down.
    public static PostgresStatementResult noResult() {
        return noResult(0);
    }
}
