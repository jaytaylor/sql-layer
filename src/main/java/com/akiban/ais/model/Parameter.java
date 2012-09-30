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
import com.akiban.server.types3.TInstance;
import java.util.concurrent.atomic.AtomicReference;

public class Parameter
{
    public static enum Direction { IN, OUT, INOUT };

    public static Parameter create(Procedure procedure, String name, Direction direction,
                                   Type type, Long typeParameter1, Long typeParameter2)
    {
        procedure.checkMutability();
        AISInvariants.checkNullName(name, "parameter", "parameter name");
        AISInvariants.checkDuplicateParametersInProcedure(procedure, name);
        Parameter parameter = new Parameter(procedure, name, direction, type, typeParameter1, typeParameter2);
        procedure.addParameter(parameter);
        return parameter;
    }

    public TInstance tInstance() {
        return new TInstance(tInstance(false));
    }

    @Override
    public String toString()
    {
        StringBuffer str = new StringBuffer(direction.name());
        if (name != null)
            str.append(" ").append(name);
        str.append( getTypeDescription());
        return str.toString();
    }

    public String getTypeDescription()
    {
        StringBuilder parameterType = new StringBuilder();
        parameterType.append(type.name());
        switch (type.nTypeParameters()) {
            case 0:
                break;
            case 1:
                String str1 = "(" + typeParameter1 + ")";
                parameterType.append(str1);
                break;
            case 2:
                String str2 = "(" + typeParameter1 + ", " + typeParameter2 + ")";
                parameterType.append(str2);
                break;
        }
        return parameterType.toString();
    }

    public Procedure getProcedure()
    {
        return procedure;
    }

    public Direction getDirection()
    {
        return direction;
    }

    public String getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    public Long getTypeParameter1()
    {
        return typeParameter1;
    }

    public Long getTypeParameter2()
    {
        return typeParameter2;
    }

    private Parameter(Procedure procedure,
                      String name,
                      Direction direction,
                      Type type,
                      Long typeParameter1,
                      Long typeParameter2)
    {
        this.procedure = procedure;
        this.name = name;
        this.direction = direction;
        this.type = type;
        this.typeParameter1 = typeParameter1;
        this.typeParameter2 = typeParameter2;
    }

    private TInstance tInstance(boolean force) {
        final TInstance old = tInstanceRef.get();
        if (old != null && !force)
            return old;
        final TInstance tinst = Column.generateTInstance(null, type, typeParameter1, typeParameter2, true);
        tInstanceRef.set(tinst);
        return tinst;
    }

    // State

    private final Procedure procedure;
    private final String name;
    private final Direction direction;
    private final Type type;
    private final Long typeParameter1;
    private final Long typeParameter2;
    private final AtomicReference<TInstance> tInstanceRef = new AtomicReference<TInstance>();
}
