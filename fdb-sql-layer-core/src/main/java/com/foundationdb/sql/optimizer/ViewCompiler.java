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

import java.util.Properties;

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
import com.foundationdb.sql.parser.CursorNode;
import com.foundationdb.sql.parser.FromSubquery;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.CursorNode.UpdateMode;
import com.foundationdb.sql.parser.ResultColumn;
import com.foundationdb.sql.parser.ResultColumnList;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.server.ServerOperatorCompiler;

public class ViewCompiler extends ServerOperatorCompiler {

    public ViewCompiler(AkibanInformationSchema ais, String defaultSchemaName, SQLParser parser,
                              TypesTranslator typesTranslator, AISBinderContext context) {
        // just enough initialization so this can be used in PlanContext
        // without the statement loader/type resolver failing
        initAIS(ais, context, defaultSchemaName);
        initParser(parser);
        initTypesTranslator(typesTranslator);
        initTypesRegistry(TypesRegistryServiceImpl.createRegistryService());
        initProperties(new Properties());
    }

    protected void initAIS(AkibanInformationSchema ais, AISBinderContext context, String defaultSchemaName) {
        super.initAIS(ais, defaultSchemaName);
        binder.setContext(context);
    }

    /** Run just enough rules to get to TypeResolver, then set types. */
    protected void findAndSetTypes(AISViewDefinition view) {
        FromSubquery fromSubquery = view.getSubquery();

        // put the SELECT in a cursorNode to enable bindAndTransform/statementLoader/etc on it.
        CursorNode cursorNode = new CursorNode();
        cursorNode.init("SELECT",
                fromSubquery.getSubquery(),
                view.getName().getFullTableName(),
                fromSubquery.getOrderByList(),
                fromSubquery.getOffset(),
                fromSubquery.getFetchFirst(),
                UpdateMode.UNSPECIFIED,
                null);
        cursorNode.setNodeType(NodeTypes.CURSOR_NODE);
        bindAndTransform(cursorNode);
        copyExposedNames(fromSubquery.getResultColumns(), fromSubquery.getSubquery().getResultColumns());
        fromSubquery.setResultColumns(fromSubquery.getSubquery().getResultColumns());

        PlanContext plan = new PlanContext(this);
        plan.setPlan(new AST(cursorNode, null));

        // can't user OperatorCompiler.compile, because it expects to return BasePlannable
        ASTStatementLoader stmtLoader = new ASTStatementLoader();
        stmtLoader.apply(plan);

        TypeResolver typeResolver = new TypeResolver();
        typeResolver.apply(plan);

        copyTypes((ResultSet) ((SelectQuery)plan.getPlan()).getInput(), fromSubquery.getResultColumns());
        
    }

    protected void copyExposedNames(ResultColumnList fromList, ResultColumnList toList) {
        int i = 0;
        if (fromList != null) {
            for (ResultColumn column : toList) {
                column.setName(fromList.get(i).getName());
                i++;
            }
        }
    }

    protected void copyTypes(ResultSet fromList, ResultColumnList toList) {
        int i = 0;
        for (ResultColumn column : toList) {
            try {
                TInstance fieldType = fromList.getFields().get(i).getType();
                if (fieldType != null)
                    column.setType(fromList.getFields().get(i).getType().dataTypeDescriptor());
            } catch (StandardException e) {
                throw new SQLParserInternalException(e);
            }
            i++;
        }
    }
}
