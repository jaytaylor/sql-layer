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

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.Parameter;
import com.akiban.ais.model.Procedure;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Type;
import com.akiban.qp.operator.QueryContext;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.InvalidProcedureException;
import com.akiban.server.error.NoSuchProcedureException;
import com.akiban.server.service.session.Session;
import com.akiban.sql.parser.CreateAliasNode;
import com.akiban.sql.parser.DropAliasNode;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.RoutineAliasInfo;
import java.sql.ParameterMetaData;

public class ProcedureDDL {
    private ProcedureDDL() { }
    
    public static void createProcedure(DDLFunctions ddlFunctions,
                                       Session session,
                                       String defaultSchemaName,
                                       CreateAliasNode createProcedure) {
        RoutineAliasInfo aliasInfo = (RoutineAliasInfo)createProcedure.getAliasInfo();
        TableName tableName = DDLHelper.convertName(defaultSchemaName, createProcedure.getObjectName());
        String schemaName = tableName.getSchemaName();
        String procedureName = tableName.getTableName();
        String language = aliasInfo.getLanguage();
        Procedure.CallingConvention callingConvention;
        if (language.equalsIgnoreCase("JAVA")) {
            switch (aliasInfo.getParameterStyle()) {
            case JAVA:
                callingConvention = Procedure.CallingConvention.JAVA;
                break;
            case AKIBAN_LOADABLE_PLAN:
                callingConvention = Procedure.CallingConvention.LOADABLE_PLAN;
                break;
            default:
                throw new InvalidProcedureException(schemaName, procedureName, "unsupported PARAMETER STYLE " + aliasInfo.getParameterStyle());
            }
        }
        else {
            throw new InvalidProcedureException(schemaName, procedureName, "unsupported LANGUAGE " + language);
        }
        AISBuilder builder = new AISBuilder();
        builder.procedure(schemaName, procedureName,
                          language, callingConvention);
        
        Long[] typeParameters = new Long[2];
        for (int i = 0; i < aliasInfo.getParameterCount(); i++) {
            String parameterName = aliasInfo.getParameterNames()[i];
            Parameter.Direction direction;
            switch (aliasInfo.getParameterModes()[i]) {
            case ParameterMetaData.parameterModeIn:
            default:
                direction = Parameter.Direction.IN;
                break;
            case ParameterMetaData.parameterModeOut:
                direction = Parameter.Direction.OUT;
                break;
            case ParameterMetaData.parameterModeInOut:
                direction = Parameter.Direction.INOUT;
                break;
            }
            Type builderType = TableDDL.columnType(aliasInfo.getParameterTypes()[i], typeParameters,
                                                   schemaName, procedureName, parameterName);
            builder.parameter(schemaName, procedureName,
                              parameterName, direction,
                              builderType.name(), typeParameters[0], typeParameters[1]);
        }
        
        if (aliasInfo.getReturnType() != null) {
            Type builderType = TableDDL.columnType(aliasInfo.getReturnType(), typeParameters,
                                                   schemaName, procedureName, "return value");
            builder.parameter(schemaName, procedureName,
                              null, Parameter.Direction.RETURN,
                              builderType.name(), typeParameters[0], typeParameters[1]);
        }

        if (createProcedure.getJavaClassName() != null) {
            String jarName = null;
            String className = createProcedure.getJavaClassName();
            String methodName = createProcedure.getMethodName();
            int idx = className.indexOf(':');
            if (idx >= 0) {
                jarName = className.substring(0, idx);
                className = className.substring(idx + 1, className.length());
            }
            builder.procedureExternalName(schemaName, procedureName,
                                          jarName, className, methodName);
        }
        else if (createProcedure.getDefinition() != null) {
            builder.procedureDefinition(schemaName, procedureName, 
                                        createProcedure.getDefinition());
        }

        Procedure procedure = builder.akibanInformationSchema().getProcedure(tableName);
        ddlFunctions.createProcedure(session, procedure);
    }

    public static void dropProcedure(DDLFunctions ddlFunctions,
                                     Session session,
                                     String defaultSchemaName,
                                     DropAliasNode dropProcedure,
                                     QueryContext context) {
        TableName procedureName = DDLHelper.convertName(defaultSchemaName, dropProcedure.getObjectName());
        Procedure procedure = ddlFunctions.getAIS(session).getProcedure(procedureName);
        
        if (procedure == null) {
            throw new NoSuchProcedureException(procedureName);
        } 
        ddlFunctions.dropProcedure(session, procedureName);
    }
}
