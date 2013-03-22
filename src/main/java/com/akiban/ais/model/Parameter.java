
package com.akiban.ais.model;

import com.akiban.ais.model.validation.AISInvariants;
import com.akiban.server.types3.TInstance;
import java.util.concurrent.atomic.AtomicReference;

public class Parameter
{
    public static enum Direction { IN, OUT, INOUT, RETURN };

    public static Parameter create(Routine routine, String name, Direction direction,
                                   Type type, Long typeParameter1, Long typeParameter2)
    {
        routine.checkMutability();
        if (name != null)
            AISInvariants.checkDuplicateParametersInRoutine(routine, name, direction);
        Parameter parameter = new Parameter(routine, name, direction, type, typeParameter1, typeParameter2);
        routine.addParameter(parameter);
        return parameter;
    }

    public TInstance tInstance() {
        return tInstance(false);
    }

    @Override
    public String toString()
    {
        StringBuffer str = new StringBuffer(direction.name());
        if (name != null)
            str.append(" ").append(name);
        str.append(" ").append(getTypeDescription());
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

    public Routine getRoutine()
    {
        return routine;
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

    private Parameter(Routine routine,
                      String name,
                      Direction direction,
                      Type type,
                      Long typeParameter1,
                      Long typeParameter2)
    {
        this.routine = routine;
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

    private final Routine routine;
    private final String name;
    private final Direction direction;
    private final Type type;
    private final Long typeParameter1;
    private final Long typeParameter2;
    private final AtomicReference<TInstance> tInstanceRef = new AtomicReference<>();
}
