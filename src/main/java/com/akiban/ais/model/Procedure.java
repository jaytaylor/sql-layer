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

import java.util.*;

public class Procedure 
{
    public static Procedure create(AkibanInformationSchema ais, 
                                   String schemaName, 
                                   String name) {
        Procedure procedure = new Procedure(ais, schemaName, name);
        ais.addProcedure(procedure);
        return procedure; 
    }
    
    protected Procedure(AkibanInformationSchema ais,
                        String schemaName, 
                        String name) {
        ais.checkMutability();
        AISInvariants.checkNullName(schemaName, "Procedure", "schema name");
        AISInvariants.checkNullName(name, "Procedure", "table name");
        AISInvariants.checkDuplicateProcedure(ais, schemaName, name);
        
        this.ais = ais;
        this.name = new TableName(schemaName, name);
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

    public Parameter getReturnValue() {
        return returnValue;
    }

    protected void checkMutability() {
        ais.checkMutability();
    }

    protected void addParameter(Parameter parameter)
    {
        switch (parameter.getDirection()) {
        case RETURN:
            returnValue = parameter;
            break;
        default:
            parameters.add(parameter);
        }
    }

    // State
    protected final AkibanInformationSchema ais;
    protected final TableName name;
    protected final List<Parameter> parameters = new ArrayList<Parameter>();
    protected Parameter returnValue = null;
}
