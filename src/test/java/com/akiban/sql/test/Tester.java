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

package com.akiban.sql.test;

import com.akiban.server.rowdata.SchemaFactory;
import com.akiban.sql.StandardException;
import com.akiban.sql.compiler.BooleanNormalizer;
import com.akiban.sql.optimizer.AISBinder;
import com.akiban.sql.optimizer.AISTypeComputer;
import com.akiban.sql.optimizer.BindingNodeFactory;
import com.akiban.sql.optimizer.BoundNodeToString;
import com.akiban.sql.optimizer.DistinctEliminator;
import com.akiban.sql.optimizer.OperatorCompiler;
import com.akiban.sql.optimizer.OperatorCompilerTest;
import com.akiban.sql.optimizer.SubqueryFlattener;
import com.akiban.sql.optimizer.plan.AST;
import com.akiban.sql.optimizer.plan.PlanToString;
import com.akiban.sql.optimizer.rule.BaseRule;
import static com.akiban.sql.optimizer.rule.DefaultRules.*;
import com.akiban.sql.optimizer.rule.PlanContext;
import com.akiban.sql.optimizer.rule.RulesContext;
import com.akiban.sql.optimizer.rule.RulesTestContext;
import com.akiban.sql.optimizer.rule.RulesTestHelper;
import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.views.ViewDefinition;

import com.akiban.ais.model.AkibanInformationSchema;

import java.util.*;
import java.io.*;

/** Standalone testing. */
public class Tester
{
    enum Action { 
        ECHO, PARSE, CLONE,
        PRINT_TREE, PRINT_SQL, PRINT_BOUND_SQL,
        BIND, COMPUTE_TYPES,
        BOOLEAN_NORMALIZE, FLATTEN_SUBQUERIES, ELIMINATE_DISTINCTS,
        PLAN, OPERATORS
    }

    List<Action> actions;
    SQLParser parser;
    Properties compilerProperties;
    BoundNodeToString unparser;
    AkibanInformationSchema ais;
    AISBinder binder;
    AISTypeComputer typeComputer;
    BooleanNormalizer booleanNormalizer;
    SubqueryFlattener subqueryFlattener;
    DistinctEliminator distinctEliminator;
    OperatorCompiler operatorCompiler;
    List<BaseRule> planRules;
    RulesContext rulesContext;
    File statsFile;
    int repeat;

    public Tester() {
        actions = new ArrayList<Action>();
        parser = new SQLParser();
        parser.setNodeFactory(new BindingNodeFactory(parser.getNodeFactory()));
        compilerProperties = new Properties();
        unparser = new BoundNodeToString();
        typeComputer = new AISTypeComputer();
        booleanNormalizer = new BooleanNormalizer(parser);
        subqueryFlattener = new SubqueryFlattener(parser);
        distinctEliminator = new DistinctEliminator(parser);
    }

    public void addAction(Action action) {
        actions.add(action);
    }

    public int getRepeat() {
        return repeat;
    }
    public void setRepeat(int repeat) {
        this.repeat = repeat;
    }

    public void process(String sql) throws Exception {
        process(sql, false);
        if (repeat > 0) {
            long start = System.currentTimeMillis();
            for (int i = 0; i < repeat; i++) {
                process(sql, true);
            }
            long end =  System.currentTimeMillis();
            System.out.println((end - start) + " ms.");
        }
    }

    public void process(String sql, boolean silent) throws Exception {
        StatementNode stmt = null;
        for (Action action : actions) {
            switch (action) {
            case ECHO:
                if (!silent) {
                    System.out.println("=====");
                    System.out.println(sql);
                }
                break;
            case PARSE:
                stmt = parser.parseStatement(sql);
                break;
            case CLONE:
                stmt = (StatementNode)parser.getNodeFactory().copyNode(stmt, parser);
                break;
            case PRINT_TREE:
                stmt.treePrint();
                break;
            case PRINT_SQL:
            case PRINT_BOUND_SQL:
                {
                    unparser.setUseBindings(action == Action.PRINT_BOUND_SQL);
                    String usql = unparser.toString(stmt);
                    if (!silent)
                        System.out.println(usql);
                }
                break;
            case BIND:
                binder.bind(stmt);
                break;
            case COMPUTE_TYPES:
                typeComputer.compute(stmt);
                break;
            case BOOLEAN_NORMALIZE:
                stmt = booleanNormalizer.normalize(stmt);
                break;
            case FLATTEN_SUBQUERIES:
                stmt = subqueryFlattener.flatten((DMLStatementNode)stmt);
                break;
            case ELIMINATE_DISTINCTS:
                stmt = distinctEliminator.eliminate((DMLStatementNode)stmt);
                break;
            case PLAN:
                {
                    PlanContext plan = 
                        new PlanContext(rulesContext, 
                                        new AST((DMLStatementNode)stmt,
                                                parser.getParameterList()));
                    rulesContext.applyRules(plan);
                    System.out.println(PlanToString.of(plan.getPlan()));
                }
                break;
            case OPERATORS:
                {
                    Object compiled = operatorCompiler.compile((DMLStatementNode)stmt,
                                                                   parser.getParameterList());
                    if (!silent)
                        System.out.println(compiled);
                }
                break;
            }
        }
    }

    static final String DEFAULT_SCHEMA = "test";

    public void setSchema(String sql) throws Exception {
        SchemaFactory schemaFactory = new SchemaFactory(DEFAULT_SCHEMA);
        ais = schemaFactory.ais(sql);
        if (actions.contains(Action.BIND))
            binder = new AISBinder(ais, DEFAULT_SCHEMA);
        if (actions.contains(Action.OPERATORS))
            operatorCompiler = OperatorCompilerTest.TestOperatorCompiler.create(parser, ais, statsFile, compilerProperties);
        if (actions.contains(Action.PLAN))
            rulesContext = RulesTestContext.create(ais, statsFile, false, planRules, compilerProperties);
    }

    public void setIndexStatistics(File file) throws Exception {
        statsFile = file;
    }

    public void defaultPlanRules() throws Exception {
        planRules = DEFAULT_RULES_CBO;
    }

    public void loadPlanRules(File file) throws Exception {
        planRules = RulesTestHelper.loadRules(file);
    }

    public void parsePlanRules(String rules) throws Exception {
        planRules = RulesTestHelper.parseRules(rules);
    }

    public void loadCompilerProperties(File file) throws Exception {
        FileInputStream fstr = new FileInputStream(file);
        try {
            compilerProperties.load(fstr);
        }
        finally {
            fstr.close();
        }
    }

    public void parseCompilerProperties(String props) throws Exception {
        compilerProperties.load(new StringReader(props));
    }

    public static String maybeFile(String sql) throws Exception {
        if (!sql.startsWith("@"))
            return sql;
        FileReader reader = null;
        try {
            reader = new FileReader(sql.substring(1));
            StringBuilder str = new StringBuilder();
            char[] buf = new char[128];
            while (true) {
                int nc = reader.read(buf);
                if (nc < 0) break;
                str.append(buf, 0, nc);
            }
            return str.toString();
        }
        finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: Tester " +
                               "[-clone] [-bind] [-types] [-boolean] [-flatten] [-plan @planfile] [-operators]" +
                               "[-tree] [-print] [-print-bound]" +
                               "[-schema ddl] [-index-stats yamlfile] [-view ddl]..." +
                               "sql...");
            System.out.println("Examples:");
            System.out.println("-tree 'SELECT t1.x+2 FROM t1'");
            System.out.println("-bind -print -tree -schema 'CREATE TABLE t1(x INT NOT NULL, y VARCHAR(7), z DECIMAL); CREATE table t2(w CHAR(1) NOT NULL);' -view 'CREATE VIEW v1(x,y) AS SELECT y,z FROM t1 WHERE y IS NOT NULL' \"SELECT x FROM v1 WHERE y > 'foo'\"");
            System.out.println("-operators -schema 'CREATE TABLE parent(id INT, PRIMARY KEY(id), name VARCHAR(256) NOT NULL, UNIQUE(name), state CHAR(2)); CREATE TABLE child(id INT, PRIMARY KEY(id), pid INT, CONSTRAINT `__akiban_fk0` FOREIGN KEY akibanfk(pid) REFERENCES parent(id), name VARCHAR(256) NOT NULL);' \"SELECT parent.name,child.name FROM parent,child WHERE child.pid = parent.id AND parent.state = 'MA'\"");

        }
        Tester tester = new Tester();
        tester.addAction(Action.ECHO);
        tester.addAction(Action.PARSE);
        int i = 0;
        while (i < args.length) {
            String arg = args[i++];
            if (arg.startsWith("-")) {
                if ("-tree".equals(arg))
                    tester.addAction(Action.PRINT_TREE);
                else if ("-print".equals(arg))
                    tester.addAction(Action.PRINT_SQL);
                else if ("-print-bound".equals(arg))
                    tester.addAction(Action.PRINT_BOUND_SQL);
                else if ("-clone".equals(arg))
                    tester.addAction(Action.CLONE);
                else if ("-bind".equals(arg))
                    tester.addAction(Action.BIND);
                else if ("-schema".equals(arg))
                    tester.setSchema(maybeFile(args[i++]));
                else if ("-index-stats".equals(arg))
                    tester.setIndexStatistics(new File(args[i++]));
                else if ("-types".equals(arg))
                    tester.addAction(Action.COMPUTE_TYPES);
                else if ("-boolean".equals(arg))
                    tester.addAction(Action.BOOLEAN_NORMALIZE);
                else if ("-flatten".equals(arg))
                    tester.addAction(Action.FLATTEN_SUBQUERIES);
                else if ("-distinct".equals(arg))
                    tester.addAction(Action.ELIMINATE_DISTINCTS);
                else if ("-plan".equals(arg)) {
                    String rules = args[i++];
                    if (rules.startsWith("@"))
                        tester.loadPlanRules(new File(rules.substring(1)));
                    else if (rules.equals("default"))
                        tester.defaultPlanRules();
                    else
                        tester.parsePlanRules(rules);
                    tester.addAction(Action.PLAN);
                }
                else if ("-compiler-properties".equals(arg)) {
                    String props = args[i++];
                    if (props.startsWith("@"))
                        tester.loadCompilerProperties(new File(props.substring(1)));
                    else
                        tester.parseCompilerProperties(props);
                }
                else if ("-operators".equals(arg))
                    tester.addAction(Action.OPERATORS);
                else if ("-repeat".equals(arg))
                    tester.setRepeat(Integer.parseInt(args[i++]));
                else
                    throw new Exception("Unknown switch: " + arg);
            }
            else {
                try {
                    tester.process(maybeFile(arg));
                }
                catch (StandardException ex) {
                    System.out.flush();
                    ex.printStackTrace();
                }
            }
        }
    }
}
