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

package com.foundationdb.server.service.routines;

import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.sql.server.ServerQueryContext;
import com.foundationdb.sql.server.ServerCallContextStack;
import com.foundationdb.sql.server.ServerSession;

/** SQL/J DDL commands are implemented as procedures in the SQLJ schema. */
public class SQLJJarRoutines
{
    private SQLJJarRoutines() {
    }

    public static void install(String url, String jar, long deploy) {
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServerSession server = context.getServer();
        TableName jarName = jarName(server, jar);
        DDLFunctions ddl = server.getDXL().ddlFunctions();
        NewAISBuilder aisb = AISBBasedBuilder.create(server.getDefaultSchemaName(),
                                                     ddl.getTypesTranslator());
        aisb.sqljJar(jarName).url(url, true);
        SQLJJar sqljJar = aisb.ais().getSQLJJar(jarName);
        ddl.createSQLJJar(server.getSession(), sqljJar);
        if (deploy != 0) {
            new SQLJJarDeployer(context, jarName).deploy();
        }
    }

    public static void replace(String url, String jar) {
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServerSession server = context.getServer();
        TableName jarName = jarName(server, jar);
        DDLFunctions ddl = server.getDXL().ddlFunctions();
        NewAISBuilder aisb = AISBBasedBuilder.create(server.getDefaultSchemaName(),
                                                     ddl.getTypesTranslator());
        aisb.sqljJar(jarName).url(url, true);
        SQLJJar sqljJar = aisb.ais().getSQLJJar(jarName);
        ddl.replaceSQLJJar(server.getSession(), sqljJar);
        server.getRoutineLoader().checkUnloadSQLJJar(server.getSession(), jarName);
    }

    public static void remove(String jar, long undeploy) {
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServerSession server = context.getServer();
        TableName jarName = jarName(server, jar);
        DDLFunctions ddl = server.getDXL().ddlFunctions();
        if (undeploy != 0) {
            new SQLJJarDeployer(context, jarName).undeploy();
        }
        ddl.dropSQLJJar(server.getSession(), jarName);
        server.getRoutineLoader().checkUnloadSQLJJar(server.getSession(), jarName);
    }

    private static TableName jarName(ServerSession server, String jar) {
        int idx = jar.lastIndexOf('.');
        if (idx < 0)
            return new TableName(server.getDefaultSchemaName(), jar);
        else
            return new TableName(jar.substring(0, idx), jar.substring(idx+1));
    }
}
