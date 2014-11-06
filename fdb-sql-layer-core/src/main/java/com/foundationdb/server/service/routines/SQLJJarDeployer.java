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

import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.error.InvalidSQLJDeploymentDescriptorException;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.aisddl.AISDDL;
import com.foundationdb.sql.aisddl.DDLHelper;
import com.foundationdb.sql.parser.CreateAliasNode;
import com.foundationdb.sql.parser.DDLStatementNode;
import com.foundationdb.sql.parser.DropAliasNode;
import com.foundationdb.sql.parser.SQLParserException;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.server.ServerQueryContext;
import com.foundationdb.sql.server.ServerSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
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
        if (jarName == null) return;
        try (JarFile jarFile = server.getRoutineLoader().openSQLJJarFile(context.getSession(), jarName)) {
            Manifest manifest = jarFile.getManifest();
            for (Map.Entry<String,Attributes> entry : manifest.getEntries().entrySet()) {
                String val = entry.getValue().getValue(MANIFEST_ATTRIBUTE);
                if ((val != null) && Boolean.parseBoolean(val)) {
                    JarEntry jarEntry = jarFile.getJarEntry(entry.getKey());
                    if (jarEntry != null) {
                        InputStream istr = jarFile.getInputStream(jarEntry);
                        loadDeploymentDescriptor(istr, undeploy);
                        break;
                    }
                }
            }
        }
        catch (IOException ex) {
            throw new InvalidSQLJDeploymentDescriptorException(jarName, ex);
        }
    }

    private void loadDeploymentDescriptor(InputStream istr, boolean undeploy) throws IOException {
        StringBuilder contents = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(istr, "UTF-8"));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;
            contents.append(line).append('\n');
        }
        if (!Pattern.compile(DESCRIPTOR_FILE, Pattern.DOTALL).matcher(contents).matches())
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
        String sql = contents.substring(start, end);
        List<StatementNode> stmts;
        try {
            stmts = server.getParser().parseStatements(sql);
        } 
        catch (SQLParserException ex) {
            throw new InvalidSQLJDeploymentDescriptorException(jarName, ex);
        }
        catch (StandardException ex) {
            throw new InvalidSQLJDeploymentDescriptorException(jarName, ex);
        }
        int nstmts = stmts.size();
        List<DDLStatementNode> ddls = new ArrayList<>(nstmts);
        List<String> sqls = new ArrayList<>(nstmts);
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
            sqls.add(sql.substring(stmt.getBeginOffset(), stmt.getEndOffset() + 1));
        }
        for (int i = 0; i < nstmts; i++) {
            AISDDL.execute(ddls.get(i), sqls.get(i), context);
        }
    }
}
