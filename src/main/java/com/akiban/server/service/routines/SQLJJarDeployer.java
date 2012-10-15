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

import com.akiban.ais.model.Routine;
import com.akiban.ais.model.SQLJJar;
import com.akiban.ais.model.TableName;
import com.akiban.server.error.InvalidSQLJDeploymentDescriptorException;
import com.akiban.sql.StandardException;
import com.akiban.sql.aisddl.AISDDL;
import com.akiban.sql.aisddl.DDLHelper;
import com.akiban.sql.parser.CreateAliasNode;
import com.akiban.sql.parser.DDLStatementNode;
import com.akiban.sql.parser.DropAliasNode;
import com.akiban.sql.parser.SQLParserException;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.server.ServerQueryContext;
import com.akiban.sql.server.ServerSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.regex.Pattern;

public class SQLJJarDeployer
{
    public static final String MANIFEST_ATTRIBUTE = "SQLJDeploymentDescriptor";
    public static final String DESCRIPTOR_FILE = "\\s*SQLActions\\s*\\[\\s*\\]\\s*\\=\\s*\\{.*\\}\\s*";
    public static final String BEGIN_INSTALL = "BEGIN INSTALL";
    public static final String END_INSTALL = "END INSTALL";
    public static final String BEGIN_REMOVE = "BEGIN REMOVE";
    public static final String END_REMOVE = "END REMOVE";

    private ServerQueryContext context;
    private TableName jarName;
    private ServerSession server;

    public SQLJJarDeployer(ServerQueryContext context, TableName jarName) {
        this.context = context;
        this.jarName = jarName;
        server = context.getServer();
    }

    public void deploy() {
        loadDeploymentDescriptor(false);
    }
   
    public void undeploy() {
        loadDeploymentDescriptor(true);
    }

    private void loadDeploymentDescriptor(boolean undeploy) {
        ClassLoader classLoader = server.getRoutineLoader().loadSQLJJar(context.getSession(), jarName);
        InputStream mstr = classLoader.getResourceAsStream("META-INF/MANIFEST.MF");
        if (mstr == null) return;
        Manifest manifest;
        try {
            manifest = new Manifest(mstr);
        }
        catch (IOException ex) {
            throw new InvalidSQLJDeploymentDescriptorException(jarName, ex);
        }
        for (Map.Entry<String,Attributes> entry : manifest.getEntries().entrySet()) {
            String val = entry.getValue().getValue(MANIFEST_ATTRIBUTE);
            if ((val != null) && Boolean.parseBoolean(val)) {
                InputStream istr = classLoader.getResourceAsStream(entry.getKey());
                if (istr != null) {
                    loadDeploymentDescriptor(istr, undeploy);
                    break;
                }
            }
        }
    }

    private void loadDeploymentDescriptor(InputStream istr, boolean undeploy) {
        StringBuilder contents = new StringBuilder();
        try {
            BufferedReader reader = 
                new BufferedReader(new InputStreamReader(istr, "UTF-8"));
            while (true) {
                String line = reader.readLine();
                if (line == null) break;
                contents.append(line).append('\n');
            }
        }
        catch (IOException ex) {
            throw new InvalidSQLJDeploymentDescriptorException(jarName, ex);
        }
        if (!Pattern.compile(DESCRIPTOR_FILE).matcher(contents).matches())
            throw new InvalidSQLJDeploymentDescriptorException(jarName, "Incorrect file format");
        String header, footer;
        if (undeploy) {
            header = BEGIN_REMOVE;
            footer = END_REMOVE;
        }
        else {
            header = BEGIN_INSTALL;
            footer = END_INSTALL;
        }
        int start = contents.indexOf(header);
        if (start < 0)
            throw new InvalidSQLJDeploymentDescriptorException(jarName, "Actions not found");
        start += header.length();
        int end = contents.indexOf(footer, start);
        if (end < 0)
            throw new InvalidSQLJDeploymentDescriptorException(jarName, "Actions not terminated");
        List<StatementNode> stmts;
        try {
            stmts = server.getParser().parseStatements(contents.substring(start, end));
        } 
        catch (SQLParserException ex) {
            throw new InvalidSQLJDeploymentDescriptorException(jarName, ex);
        }
        catch (StandardException ex) {
            throw new InvalidSQLJDeploymentDescriptorException(jarName, ex);
        }
        List<DDLStatementNode> ddls = new ArrayList<DDLStatementNode>();
        for (StatementNode stmt : stmts) {
            boolean stmtOkay = false, thisjarOkay = false;
            if (undeploy) {
                if (stmt instanceof DropAliasNode) {
                    DropAliasNode dropAlias = (DropAliasNode)stmt;
                    switch (dropAlias.getAliasType()) {
                    case PROCEDURE:
                    case FUNCTION:
                        stmtOkay = true;
                        {
                            TableName routineName = DDLHelper.convertName(server.getDefaultSchemaName(), dropAlias.getObjectName());
                            Routine routine = server.getAIS().getRoutine(routineName);
                            if (routine != null) {
                                SQLJJar sqljjar = routine.getSQLJJar();
                                thisjarOkay = ((sqljjar != null) && 
                                               jarName.equals(sqljjar.getName()));
                            }
                        }
                        break;
                    }
                }
            }
            else {
                if (stmt instanceof CreateAliasNode) {
                    CreateAliasNode createAlias = (CreateAliasNode)stmt;
                    switch (createAlias.getAliasType()) {
                    case PROCEDURE:
                    case FUNCTION:
                        stmtOkay = true;
                        if ((createAlias.getJavaClassName() != null) &&
                            createAlias.getJavaClassName().startsWith("thisjar:")) {
                            createAlias.setUserData(jarName);
                            thisjarOkay = true;
                        }
                        break;
                    }
                }
            }
            if (!stmtOkay)
                throw new InvalidSQLJDeploymentDescriptorException(jarName, "Statement not allowed " + stmt.statementToString());
            if (!thisjarOkay)
                throw new InvalidSQLJDeploymentDescriptorException(jarName, "Must refer to thisjar:");
            ddls.add((DDLStatementNode)stmt);
        }
        for (DDLStatementNode ddl : ddls) {
            AISDDL.execute(ddl, context);
        }
    }
}
