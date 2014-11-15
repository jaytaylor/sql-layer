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
package com.foundationdb.sql.aisddl;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.Parameter;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.error.InvalidRoutineException;
import com.foundationdb.server.error.NoSuchRoutineException;
import com.foundationdb.server.error.NoSuchSQLJJarException;
import com.foundationdb.server.service.routines.RoutineLoader;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.sql.parser.CreateAliasNode;
import com.foundationdb.sql.parser.DropAliasNode;
import com.foundationdb.sql.types.RoutineAliasInfo;

import java.sql.ParameterMetaData;

import static com.foundationdb.sql.aisddl.DDLHelper.skipOrThrow;

public class RoutineDDL {
    private RoutineDDL() { }
    
    static class ParameterStyleCallingConvention {
        final String language, parameterStyle;
        final Routine.CallingConvention callingConvention;
        
        ParameterStyleCallingConvention(String language, String parameterStyle,
                                        Routine.CallingConvention callingConvention) {
            this.language = language;
            this.parameterStyle = parameterStyle;
            this.callingConvention = callingConvention;
        }
    }

    static final ParameterStyleCallingConvention[] parameterStyleCallingConventions = {
        new ParameterStyleCallingConvention("JAVA", "JAVA", 
                                            Routine.CallingConvention.JAVA),
        new ParameterStyleCallingConvention("JAVA", "FOUNDATIONDB_LOADABLE_PLAN",
                                            Routine.CallingConvention.LOADABLE_PLAN),
        new ParameterStyleCallingConvention(null, "VARIABLES", 
                                            Routine.CallingConvention.SCRIPT_BINDINGS),
        new ParameterStyleCallingConvention(null, "JAVA", 
                                            Routine.CallingConvention.SCRIPT_FUNCTION_JAVA),
        new ParameterStyleCallingConvention(null, "JSON", 
                                            Routine.CallingConvention.SCRIPT_BINDINGS_JSON),
        new ParameterStyleCallingConvention(null, "LIBRARY", 
                                            Routine.CallingConvention.SCRIPT_LIBRARY),
    };

    protected static Routine.CallingConvention findCallingConvention(String schemaName,
                                                                     String routineName,
                                                                     String language,
                                                                     String parameterStyle,
                                                                     RoutineLoader routineLoader,
                                                                     Session session) {
        boolean languageSeen = false, isScript = false, scriptChecked = false;
        for (ParameterStyleCallingConvention cc : parameterStyleCallingConventions) {
            if (cc.language == null) {
                if (!scriptChecked) {
                    isScript = routineLoader.isScriptLanguage(session, language);
                    scriptChecked = true;
                }
                if (!isScript) continue;
            }
            else if (cc.language.equalsIgnoreCase(language)) {
                languageSeen = true;
            }
            else {
                continue;
            }
            if (cc.parameterStyle.equalsIgnoreCase(parameterStyle)) {
                return cc.callingConvention;
            }
        }
        if (languageSeen) {
            throw new InvalidRoutineException(schemaName, routineName, "unsupported PARAMETER STYLE " + parameterStyle);
        }
        else {
            throw new InvalidRoutineException(schemaName, routineName, "unsupported LANGUAGE " + language);
        }
    }

    public static void createRoutine(DDLFunctions ddlFunctions,
                                     RoutineLoader routineLoader,
                                     Session session,
                                     String defaultSchemaName,
                                     CreateAliasNode createAlias) {
        RoutineAliasInfo aliasInfo = (RoutineAliasInfo)createAlias.getAliasInfo();
        TableName tableName = DDLHelper.convertName(defaultSchemaName, createAlias.getObjectName());
        String schemaName = tableName.getSchemaName();
        String routineName = tableName.getTableName();
        String language = aliasInfo.getLanguage();
        Routine.CallingConvention callingConvention = findCallingConvention(schemaName, routineName, language, aliasInfo.getParameterStyle(),
                                                                            routineLoader, session);
        switch (callingConvention) {
        case SCRIPT_BINDINGS:
        case SCRIPT_BINDINGS_JSON:
        case SCRIPT_LIBRARY:
            if (createAlias.getExternalName() != null)
                throw new InvalidRoutineException(schemaName, routineName, language + " routine cannot have EXTERNAL NAME");
            break;
        case SCRIPT_FUNCTION_JAVA:
        case SCRIPT_FUNCTION_JSON:
            if (createAlias.getExternalName() == null) {
                throw new InvalidRoutineException(schemaName, routineName, "must have EXTERNAL NAME function_name");
            }
        default:
            break;
        }

        TypesTranslator typesTranslator = ddlFunctions.getTypesTranslator();
        AISBuilder builder = new AISBuilder();
        builder.routine(schemaName, routineName,
                        language, callingConvention);
        
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
            TInstance type = typesTranslator.typeForSQLType(aliasInfo.getParameterTypes()[i],
                    schemaName, routineName, parameterName);
            builder.parameter(schemaName, routineName, parameterName,
                              direction, type);
        }
        
        if (aliasInfo.getReturnType() != null) {
            TInstance type = typesTranslator.typeForSQLType(aliasInfo.getReturnType(),
                    schemaName, routineName, "return value");
            builder.parameter(schemaName, routineName, null,
                              Parameter.Direction.RETURN, type);
        }

        if (createAlias.getExternalName() != null) {
            String className, methodName;
            boolean checkJarName;
            if (callingConvention == Routine.CallingConvention.JAVA) {
                className = createAlias.getJavaClassName();
                methodName = createAlias.getMethodName();
                checkJarName = true;
            }
            else if (callingConvention == Routine.CallingConvention.LOADABLE_PLAN) {
                // The whole class implements a standard interface.
                className = createAlias.getExternalName();
                methodName = null;
                checkJarName = true;
            }
            else {
                className = null;
                methodName = createAlias.getExternalName();
                checkJarName = false;
            }
            String jarSchema = null;
            String jarName = null;
            if (checkJarName) {
                int idx = className.indexOf(':');
                if (idx >= 0) {
                    jarName = className.substring(0, idx);
                    className = className.substring(idx + 1);
                    if (jarName.equals("thisjar")) {
                        TableName thisJar = (TableName)createAlias.getUserData();
                        jarSchema = thisJar.getSchemaName();
                        jarName = thisJar.getTableName();
                    }
                    else {
                        idx = jarName.indexOf('.');
                        if (idx < 0) {
                            jarSchema = defaultSchemaName;
                        }
                        else {
                            jarSchema = jarName.substring(0, idx);
                            jarName = jarName.substring(idx + 1);
                        }
                    }
                }
            }
            if (jarName != null) {
                AkibanInformationSchema ais = ddlFunctions.getAIS(session);
                SQLJJar sqljJar = ais.getSQLJJar(jarSchema, jarName);
                if (sqljJar == null)
                    throw new NoSuchSQLJJarException(jarSchema, jarName);
                builder.sqljJar(jarSchema, jarName, sqljJar.getURL());
            }
            builder.routineExternalName(schemaName, routineName, 
                                        jarSchema, jarName, 
                                        className, methodName);
        }
        if (createAlias.getDefinition() != null) {
            builder.routineDefinition(schemaName, routineName, 
                                      createAlias.getDefinition());
        }

        if (aliasInfo.getSQLAllowed() != null) {
            Routine.SQLAllowed sqlAllowed;
            switch (aliasInfo.getSQLAllowed()) {
            case MODIFIES_SQL_DATA:
                sqlAllowed = Routine.SQLAllowed.MODIFIES_SQL_DATA;
                break;
            case READS_SQL_DATA:
                sqlAllowed = Routine.SQLAllowed.READS_SQL_DATA;
                break;
            case CONTAINS_SQL:
                sqlAllowed = Routine.SQLAllowed.CONTAINS_SQL;
                break;
            case NO_SQL:
                sqlAllowed = Routine.SQLAllowed.NO_SQL;
                break;
            default:
                throw new InvalidRoutineException(schemaName, routineName, "unsupported " + aliasInfo.getSQLAllowed().getSQL());
            }
            builder.routineSQLAllowed(schemaName, routineName, sqlAllowed);
        }
        builder.routineDynamicResultSets(schemaName, routineName,
                                         aliasInfo.getMaxDynamicResultSets());
        builder.routineDeterministic(schemaName, routineName,
                                     aliasInfo.isDeterministic());
        builder.routineCalledOnNullInput(schemaName, routineName,
                                         aliasInfo.calledOnNullInput());
        
        Routine routine = builder.akibanInformationSchema().getRoutine(tableName);
        boolean replaceExisting = createAlias.isCreateOrReplace();
        ddlFunctions.createRoutine(session, routine, replaceExisting);
        if (replaceExisting)
            routineLoader.checkUnloadRoutine(session, tableName);
    }

    public static void dropRoutine(DDLFunctions ddlFunctions,
                                   RoutineLoader routineLoader,
                                   Session session,
                                   String defaultSchemaName,
                                   DropAliasNode dropRoutine,
                                   QueryContext context) {
        TableName routineName = DDLHelper.convertName(defaultSchemaName, dropRoutine.getObjectName());
        Routine routine = ddlFunctions.getAIS(session).getRoutine(routineName);

        if((routine == null) &&
           skipOrThrow(context, dropRoutine.getExistenceCheck(), routine, new NoSuchRoutineException(routineName))) {
            return;
        }

        ddlFunctions.dropRoutine(session, routineName);
        routineLoader.checkUnloadRoutine(session, routineName);
    }
}
