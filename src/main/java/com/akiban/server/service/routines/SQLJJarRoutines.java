/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.service.routines;

import com.akiban.ais.model.SQLJJar;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.sql.server.ServerQueryContext;
import com.akiban.sql.server.ServerCallContextStack;
import com.akiban.sql.server.ServerSession;

/** SQL/J DDL commands are implemented as procedures in the SQLJ schema. */
public class SQLJJarRoutines
{
    private SQLJJarRoutines() {
    }

    public static void install(String url, String jar, long deploy) {
        ServerQueryContext context = ServerCallContextStack.current().getContext();
        ServerSession server = context.getServer();
        TableName jarName = jarName(server, jar);
        NewAISBuilder aisb = AISBBasedBuilder.create(server.getDefaultSchemaName());
        aisb.sqljJar(jarName).url(url);
        SQLJJar sqljJar = aisb.ais().getSQLJJar(jarName);
        server.getDXL().ddlFunctions().createSQLJJar(server.getSession(), sqljJar);
        if (deploy != 0) {
            new SQLJJarDeployer(context, jarName).deploy();
        }
    }

    public static void replace(String url, String jar) {
        ServerQueryContext context = ServerCallContextStack.current().getContext();
        ServerSession server = context.getServer();
        TableName jarName = jarName(server, jar);
        server.getRoutineLoader().unloadSQLJJar(jarName);
        NewAISBuilder aisb = AISBBasedBuilder.create(server.getDefaultSchemaName());
        aisb.sqljJar(jarName).url(url);
        SQLJJar sqljJar = aisb.ais().getSQLJJar(jarName);
        server.getDXL().ddlFunctions().replaceSQLJJar(server.getSession(), sqljJar);
    }

    public static void remove(String jar, long undeploy) {
        ServerQueryContext context = ServerCallContextStack.current().getContext();
        ServerSession server = context.getServer();
        TableName jarName = jarName(server, jar);
        if (undeploy != 0) {
            new SQLJJarDeployer(context, jarName).undeploy();
        }
        server.getRoutineLoader().unloadSQLJJar(jarName);
        server.getDXL().ddlFunctions().dropSQLJJar(server.getSession(), jarName);
    }

    private static TableName jarName(ServerSession server, String jar) {
        int idx = jar.lastIndexOf('.');
        if (idx < 0)
            return new TableName(server.getDefaultSchemaName(), jar);
        else
            return new TableName(jar.substring(0, idx), jar.substring(idx+1));
    }
}
