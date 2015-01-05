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

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.service.monitor.SessionMonitor.StatementTypes;
import com.foundationdb.sql.optimizer.plan.CostEstimate;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StatementNode;

import java.io.IOException;
import java.util.List;

/**
 * Generally trivial handling of session verbs that aren't in the actual SQL grammar.
 */
public class PostgresEmulatedSessionStatement implements PostgresStatement
{
    enum Verb {
        UNLISTEN("UNLISTEN"),
        DISCARD("DISCARD");

        private String sql;

        Verb(String sql) {
            this.sql = sql;
        }

        public String getSQL() {
            return sql;
        }
    }

    private Verb verb;
    private String sql;
    private long aisGeneration;

    protected PostgresEmulatedSessionStatement(Verb verb, String sql) {
        this.verb = verb;
        this.sql = sql;
    }

    @Override
    public PostgresType[] getParameterTypes() {
        return null;
    }

    @Override
    public void sendDescription(PostgresQueryContext context,
                                boolean always, boolean params)
            throws IOException {
        if (always) {
            PostgresServerSession server = context.getServer();
            PostgresMessenger messenger = server.getMessenger();
            messenger.beginMessage(PostgresMessages.NO_DATA_TYPE.code());
            messenger.sendMessage();
        }
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.ALLOWED;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.ALLOWED;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.ALLOWED;
    }

    @Override
    public int execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        server.getSessionMonitor().countEvent(StatementTypes.SESSION);
        doOperation(context, server);
        {        
            PostgresMessenger messenger = server.getMessenger();
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            messenger.writeString(verb.getSQL());
            messenger.sendMessage();
        }
        return 0;
    }

    @Override
    public boolean hasAISGeneration() {
        return aisGeneration != 0;
    }

    @Override
    public void setAISGeneration(long aisGeneration) {
        this.aisGeneration = aisGeneration;
    }

    @Override
    public long getAISGeneration() {
        return aisGeneration;
    }

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        return this;
    }

    @Override
    public boolean putInCache() {
        return false;
    }

    @Override
    public CostEstimate getCostEstimate() {
        return null;
    }

    protected void doOperation(PostgresQueryContext context, PostgresServerSession server) throws IOException {
        switch (verb) {
        case UNLISTEN:
        default:
            break;
        }
    }

}
