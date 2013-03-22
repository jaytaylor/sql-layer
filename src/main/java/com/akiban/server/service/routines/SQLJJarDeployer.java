
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
