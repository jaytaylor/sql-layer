
package com.akiban.sql.server;

import com.akiban.ais.model.Parameter;
import com.akiban.ais.model.Routine;
import com.akiban.ais.model.TableName;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;

public abstract class ServerRoutineInvocation
{
    private final Routine routine;

    protected ServerRoutineInvocation(Routine routine) {
        this.routine = routine;
    }

    public int size() {
        return routine.getParameters().size();
    }

    public Routine getRoutine() {
        return routine;
    }

    public Routine.CallingConvention getCallingConvention() {
        return routine.getCallingConvention();
    }

    public TableName getRoutineName() {
        return routine.getName();
    }

    public Parameter getRoutineParameter(int index) {
        if (index == ServerJavaValues.RETURN_VALUE_INDEX)
            return routine.getReturnValue();
        else
            return routine.getParameters().get(index);
    }

    protected AkType getAkType(int index) {
        return getRoutineParameter(index).getType().akType();
    }

    protected TInstance getTInstance(int index) {
        return getRoutineParameter(index).tInstance();
    }

    public abstract ServerJavaValues asValues(ServerQueryContext queryContext);
    
}
