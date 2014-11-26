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
package com.foundationdb.ais.model;

import com.foundationdb.ais.model.validation.AISInvariants;
import com.foundationdb.server.error.InvalidRoutineException;

import java.util.*;

public class Routine 
{
    public static enum CallingConvention {
        JAVA, LOADABLE_PLAN, SQL_ROW, SCRIPT_FUNCTION_JAVA, SCRIPT_BINDINGS, 
        SCRIPT_FUNCTION_JSON, SCRIPT_BINDINGS_JSON, SCRIPT_LIBRARY
    }

    public static enum SQLAllowed {
        MODIFIES_SQL_DATA, READS_SQL_DATA, CONTAINS_SQL, NO_SQL
    }

    public static Routine create(AkibanInformationSchema ais, 
                                 String schemaName, String name,
                                 String language, CallingConvention callingConvention) {
        Routine routine = new Routine(ais, schemaName, name, language, callingConvention);
        ais.addRoutine(routine);
        return routine; 
    }
    
    protected Routine(AkibanInformationSchema ais,
                      String schemaName, String name,
                      String language, CallingConvention callingConvention) {
        ais.checkMutability();
        AISInvariants.checkNullName(schemaName, "Routine", "schema name");
        AISInvariants.checkNullName(name, "Routine", "table name");
        AISInvariants.checkDuplicateRoutine(ais, schemaName, name);
        
        this.ais = ais;
        this.name = new TableName(schemaName, name);
        this.language = language;
        this.callingConvention = callingConvention;
    }
    
    public TableName getName() {
        return name;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public Parameter getNamedParameter(String name) {
        for (Parameter parameter : parameters) {
            if (name.equals(parameter.getName())) {
                return parameter;
            }
        }
        return null;
    }

    public boolean isProcedure() {
        return (returnValue == null);
    }

    public Parameter getReturnValue() {
        return returnValue;
    }

    public String getLanguage() {
        return language;
    }

    public CallingConvention getCallingConvention() {
        return callingConvention;
    }

    public SQLJJar getSQLJJar() {
        return sqljJar;
    }

    public String getClassName() {
        return className;
    }

    public String getMethodName() {
        return methodName;
    }

    public String getExternalName() {
        if (methodName == null)
            return className;
        else if (className == null)
            return methodName;
        else
            return className + "." + methodName;
    }

    public String getDefinition() {
        return definition;
    }

    public SQLAllowed getSQLAllowed() {
        return sqlAllowed;
    }

    public int getDynamicResultSets() {
        return dynamicResultSets;
    }

    public boolean isDeterministic() {
        return deterministic;
    }

    public boolean isCalledOnNullInput() {
        return calledOnNullInput;
    }

    protected void checkMutability() {
        ais.checkMutability();
    }
    
    public boolean isSystemRoutine() {
        if (name.getSchemaName().equalsIgnoreCase("sys") ||
                name.getSchemaName().equalsIgnoreCase("security_schema") ||
                name.getSchemaName().equalsIgnoreCase("sqlj")) {
            return true;
        }
        return false;
    }

    protected void addParameter(Parameter parameter)
    {
        checkMutability();
        switch (parameter.getDirection()) {
        case RETURN:
            returnValue = parameter;
            break;
        default:
            parameters.add(parameter);
        }
    }

    public void setExternalName(SQLJJar sqljJar, String className, String methodName) {
        checkMutability();
        switch (callingConvention) {
        case JAVA:
            AISInvariants.checkNullName(className, "Routine", "class name");
            AISInvariants.checkNullName(methodName, "Routine", "method name");
            break;
        case LOADABLE_PLAN:
            AISInvariants.checkNullName(className, "Routine", "class name");
            break;
        case SCRIPT_FUNCTION_JAVA:
        case SCRIPT_FUNCTION_JSON:
            AISInvariants.checkNullName(methodName, "Routine", "function name");
            break;
        default:
            throw new InvalidRoutineException(name.getSchemaName(), name.getTableName(), 
                                              "EXTERNAL NAME not allowed for " + callingConvention);
        }
        this.sqljJar = sqljJar;
        if (sqljJar != null)
            sqljJar.addRoutine(this);
        this.className = className;
        this.methodName = methodName;
    }

    public void setDefinition(String definition) {
        checkMutability();
        switch (callingConvention) {
        case JAVA:
        case LOADABLE_PLAN:
            throw new InvalidRoutineException(name.getSchemaName(), name.getTableName(), 
                                              "AS not allowed for " + callingConvention);
        }
        this.definition = definition;
    }

    public void setSQLAllowed(SQLAllowed sqlAllowed) {
        checkMutability();
        this.sqlAllowed = sqlAllowed;
    }

    public void setDynamicResultSets(int dynamicResultSets) {
        checkMutability();
        this.dynamicResultSets = dynamicResultSets;
    }

    public void setDeterministic(boolean deterministic) {
        checkMutability();
        this.deterministic = deterministic;
    }

    public void setCalledOnNullInput(boolean calledOnNullInput) {
        checkMutability();
        this.calledOnNullInput = calledOnNullInput;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    // State
    protected final AkibanInformationSchema ais;
    protected final TableName name;
    protected final List<Parameter> parameters = new ArrayList<>();
    protected Parameter returnValue = null;
    protected String language;
    protected CallingConvention callingConvention;
    protected SQLJJar sqljJar;
    protected String className, methodName;
    protected String definition;
    protected SQLAllowed sqlAllowed;
    protected int dynamicResultSets = 0;
    protected boolean deterministic, calledOnNullInput;
    protected long version;
}
