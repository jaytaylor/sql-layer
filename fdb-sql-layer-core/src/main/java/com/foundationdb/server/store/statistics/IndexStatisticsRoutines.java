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

package com.foundationdb.server.store.statistics;

import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.sql.server.ServerCallContextStack;
import com.foundationdb.sql.server.ServerQueryContext;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.concurrent.Callable;

@SuppressWarnings("unused") // reflection
public class IndexStatisticsRoutines
{
    public static void delete(final String schema) {
        txnService().run(session(), new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                indexService().deleteIndexStatistics(session(), getSchema(schema));
                return null;
            }
        });
    }

    public static String dumpToFile(String schema, String toFile) throws IOException {
        File file = new File(toFile);
        try (FileWriter writer = new FileWriter(file)) {
            dumpInternal(writer, getSchema(schema));
        }
        return file.getAbsolutePath();
    }

    public static String dumpToString(String schema) throws IOException {
        StringWriter writer = new StringWriter();
        dumpInternal(writer, getSchema(schema));
        writer.close();
        return writer.toString();
    }

    public static void loadFromFile(final String schema, final String fromFile)  throws IOException {
        txnService().run(session(), new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                File file = new File(fromFile);
                indexService().loadIndexStatistics(session(), getSchema(schema), file);
                return null;
            }
        });
    }

    //
    // Internal
    //

    private IndexStatisticsRoutines() {
    }

    private static IndexStatisticsService indexService() {
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        return context.getServer().getServiceManager().getServiceByClass(IndexStatisticsService.class);
    }

    private static Session session() {
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        return context.getServer().getSession();
    }

    private static TransactionService txnService() {
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        return context.getServer().getTransactionService();
    }

    private static String getSchema(String schemaInput) {
        return (schemaInput != null) ? schemaInput : ServerCallContextStack.getCallingContext().getCurrentSchema();
    }

    private static void dumpInternal(final Writer writer, final String schema) throws IOException {
        txnService().run(session(), new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                indexService().dumpIndexStatistics(session(), getSchema(schema), writer);
                return null;
            }
        });
    }
}
