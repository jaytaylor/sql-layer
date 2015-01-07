/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
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

package com.foundationdb.sql.optimizer;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.server.error.SQLParserInternalException;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.service.TypesRegistryServiceImpl;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.optimizer.plan.AST;
import com.foundationdb.sql.optimizer.plan.ResultSet;
import com.foundationdb.sql.optimizer.plan.SelectQuery;
import com.foundationdb.sql.optimizer.rule.ASTStatementLoader;
import com.foundationdb.sql.optimizer.rule.PlanContext;
import com.foundationdb.sql.optimizer.rule.TypeResolver;
import com.foundationdb.sql.parser.CreateViewNode;
import com.foundationdb.sql.parser.CursorNode;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.CursorNode.UpdateMode;
import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.ResultColumn;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.server.ServerOperatorCompiler;

public class ViewCompiler extends ServerOperatorCompiler {

    public ViewCompiler(AkibanInformationSchema ais, String defaultSchemaName, SQLParser parser,
                              TypesTranslator typesTranslator) {
        initAIS(ais, defaultSchemaName);
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
    protected void compile(CreateViewNode createViewNode) {
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
            } catch (StandardException e) {
                throw new SQLParserInternalException(e);
            }
            i++;
        }
    }

    /** Apply AST-level transformations before rules. */
    @Override
    protected DMLStatementNode bindAndTransform(DMLStatementNode stmt)  {
        try {
            stmt = (DMLStatementNode)booleanNormalizer.normalize(stmt);
            stmt = subqueryFlattener.flatten(stmt);
            return stmt;
        }
        catch (StandardException e) {
            throw new SQLParserInternalException(e);
        }
    }
}
