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

package com.akiban.sql.aisddl;

import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.*;
import com.akiban.server.service.session.Session;

import com.akiban.sql.optimizer.AISBinderContext;
import com.akiban.sql.optimizer.AISViewDefinition;
import com.akiban.sql.parser.CreateViewNode;
import com.akiban.sql.parser.DropViewNode;
import com.akiban.sql.parser.ExistenceCheck;
import com.akiban.sql.parser.ResultColumn;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Columnar;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Types;
import com.akiban.ais.model.View;

import java.util.Collection;
import java.util.Map;

/** DDL operations on Views */
public class ViewDDL
{
    private ViewDDL() {
    }

    public static void createView(DDLFunctions ddlFunctions,
                                  Session session,
                                  String defaultSchemaName,
                                  CreateViewNode createView,
                                  AISBinderContext binderContext) {
        com.akiban.sql.parser.TableName parserName = createView.getObjectName();
        String schemaName = parserName.hasSchema() ? parserName.getSchemaName() : defaultSchemaName;
        String viewName = parserName.getTableName();
        ExistenceCheck condition = createView.getExistenceCheck();

        if (ddlFunctions.getAIS(session).getView(schemaName, viewName) != null) {
            switch(condition) {
            case IF_NOT_EXISTS:
                // view already exists. does nothing
                return;
            case NO_CONDITION:
                throw new DuplicateViewException(schemaName, viewName);
            default:
                throw new IllegalStateException("Unexpected condition: " + condition);
            }
        }
        
        AISViewDefinition viewdef = binderContext.getViewDefinition(createView);
        Map<Columnar,Collection<Column>> tableColumnReferences = viewdef.getTableColumnReferences();
        AISBuilder builder = new AISBuilder();
        builder.view(schemaName, viewName, viewdef.getQueryExpression(), 
                     binderContext.getParserProperties(schemaName), tableColumnReferences);
        int colpos = 0;
        for (ResultColumn rc : viewdef.getResultColumns()) {
            TableDDL.addColumn(builder, schemaName, viewName, rc.getName(), colpos++,
                               rc.getType(), false);
        }
        View view = builder.akibanInformationSchema().getView(schemaName, viewName);
        ddlFunctions.createView(session, view);
    }

    public static void dropView (DDLFunctions ddlFunctions,
                                 Session session, 
                                 String defaultSchemaName,
                                 DropViewNode dropView,
                                 AISBinderContext binderContext) {
        com.akiban.sql.parser.TableName parserName = dropView.getObjectName();
        String schemaName = parserName.hasSchema() ? parserName.getSchemaName() : defaultSchemaName;
        TableName viewName = TableName.create(schemaName, parserName.getTableName());
        ExistenceCheck existenceCheck = dropView.getExistenceCheck();

        if (ddlFunctions.getAIS(session).getView(viewName) == null) {
            if (existenceCheck == ExistenceCheck.IF_EXISTS)
                return;
            throw new UndefinedViewException(viewName);
        }
        checkDropTable(ddlFunctions, session, viewName);
        ddlFunctions.dropView(session, viewName);
    }

    public static void checkDropTable(DDLFunctions ddlFunctions, Session session, 
                                      TableName name) {
        AkibanInformationSchema ais = ddlFunctions.getAIS(session);
        Columnar table = ais.getColumnar(name);
        if (table == null) return;
        for (View view : ais.getViews().values()) {
            if (view.referencesTable(table)) {
                throw new ViewReferencesExist(view.getName().getSchemaName(),
                                              view.getName().getTableName(),
                                              table.getName().getSchemaName(),
                                              table.getName().getTableName());
            }
        }
    }

}
