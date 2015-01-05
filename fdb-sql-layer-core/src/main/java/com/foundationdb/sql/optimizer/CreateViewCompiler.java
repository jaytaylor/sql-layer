package com.foundationdb.sql.optimizer;

import java.util.Arrays;
import java.util.List;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.server.error.SQLParserInternalException;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.service.TypesRegistryServiceImpl;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.optimizer.plan.AST;
import com.foundationdb.sql.optimizer.plan.BasePlannable;
import com.foundationdb.sql.optimizer.plan.ResultSet;
import com.foundationdb.sql.optimizer.plan.SelectQuery;
import com.foundationdb.sql.optimizer.rule.ASTStatementLoader;
import com.foundationdb.sql.optimizer.rule.BaseRule;
import com.foundationdb.sql.optimizer.rule.OperatorAssembler;
import com.foundationdb.sql.optimizer.rule.PlanContext;
import com.foundationdb.sql.optimizer.rule.TypeResolver;
import com.foundationdb.sql.optimizer.rule.ConstantFolder.Folder;
import com.foundationdb.sql.parser.CreateViewNode;
import com.foundationdb.sql.parser.CursorNode;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.CursorNode.UpdateMode;
import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.ResultColumn;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.server.ServerOperatorCompiler;

public class CreateViewCompiler extends ServerOperatorCompiler {

    /** These are the rules that get run for view compilation. */
    public static final List<BaseRule> VIEW_RULES = Arrays.asList(
            // These aren't singletons because someday they will have options.
            new ASTStatementLoader(),
            new TypeResolver()
    );

    public CreateViewCompiler(AkibanInformationSchema ais, String defaultSchemaName, SQLParser parser,
                              TypesTranslator typesTranslator) {
        initAIS(ais, defaultSchemaName);
        initRules(VIEW_RULES);
        initParser(parser);
        initTypesTranslator(typesTranslator);
        initTypesRegistry(TypesRegistryServiceImpl.createRegistryService());
    }

    @Override
    protected void initAIS(AkibanInformationSchema ais, String defaultSchemaName) {
        super.initAIS(ais, defaultSchemaName);
        binder.setAllowSubqueryMultipleColumns(true);
    }

    /** Compile a statement into an operator tree. */
    public BasePlannable compile(DMLStatementNode stmt, List<ParameterNode> params) {
        return compile(stmt, params, new PlanContext(this));
    }

    public BasePlannable compile(DMLStatementNode stmt, List<ParameterNode> params,
                                 PlanContext plan) {
        stmt = bindAndTransform(stmt); // Get into standard form.
        plan.setPlan(new AST(stmt, params));
        applyRules(plan);
        return (BasePlannable)plan.getPlan();
    }

    /** Compile a statement into an operator tree. */
    protected void compile(CreateViewNode createViewNode, AISBinderContext context) {
        CursorNode cursorNode = new CursorNode();
        cursorNode.init("SELECT",
                createViewNode.getParsedQueryExpression(),
                createViewNode.getFullName(),
                createViewNode.getOrderByList(),
                createViewNode.getOffset(),
                createViewNode.getFetchFirst(),
                UpdateMode.UNSPECIFIED,
                null);
        cursorNode.setNodeType(NodeTypes.CURSOR_NODE);
        
        PlanContext plan = new PlanContext(this);
        binder.setContext(context);
        bindAndTransform(cursorNode);
        plan.setPlan(new AST(cursorNode, null));
        
        ASTStatementLoader stmtLoader = new ASTStatementLoader();
        stmtLoader.apply(plan);
        
        TypeResolver typeResolver = new TypeResolver();
        typeResolver.apply(plan);
        
        ResultSet resultSet = (ResultSet) ((SelectQuery)plan.getPlan()).getInput();
        int i = 0;
        for (ResultColumn column : createViewNode.getParsedQueryExpression().getResultColumns()) {
            try {
                TInstance fieldType = resultSet.getFields().get(i).getType();
                if (fieldType != null)
                    column.setType(resultSet.getFields().get(i).getType().dataTypeDescriptor());
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            i++;
        }
    }

    /** Apply AST-level transformations before rules. */
    @Override
    protected DMLStatementNode bindAndTransform(DMLStatementNode stmt)  {
        try {
            binder.bind(stmt);
            stmt = (DMLStatementNode)booleanNormalizer.normalize(stmt);
            typeComputer.compute(stmt);
            stmt = subqueryFlattener.flatten(stmt);
            return stmt;
        } 
        catch (StandardException ex) {
            throw new SQLParserInternalException(ex);
        }
    }
}
