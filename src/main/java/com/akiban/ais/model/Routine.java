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
package com.akiban.ais.model;

import com.akiban.ais.model.validation.AISInvariants;
import com.akiban.server.error.InvalidRoutineException;

import java.util.*;

public class Routine 
{
    public static enum CallingConvention {
        JAVA, LOADABLE_PLAN
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

    protected void checkMutability() {
        ais.checkMutability();
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
        AISInvariants.checkNullName(className, "Routine", "class name");
        switch (callingConvention) {
        case JAVA:
            AISInvariants.checkNullName(methodName, "Routine", "method name");
            break;
        case LOADABLE_PLAN:
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
        this.sqlAllowed = sqlAllowed;
    }

    public void setDynamicResultSets(int dynamicResultSets) {
        this.dynamicResultSets = dynamicResultSets;
    }

    // State
    protected final AkibanInformationSchema ais;
    protected final TableName name;
    protected final List<Parameter> parameters = new ArrayList<Parameter>();
    protected Parameter returnValue = null;
    protected String language;
    protected CallingConvention callingConvention;
    protected SQLJJar sqljJar;
    protected String className, methodName;
    protected String definition;
    protected SQLAllowed sqlAllowed;
    protected int dynamicResultSets = 0;
}
