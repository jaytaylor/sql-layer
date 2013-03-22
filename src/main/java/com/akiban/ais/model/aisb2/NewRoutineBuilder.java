
package com.akiban.ais.model.aisb2;

import static com.akiban.ais.model.Routine.*;

public interface NewRoutineBuilder {
    NewRoutineBuilder language(String language, CallingConvention callingConvention);
    
    NewRoutineBuilder paramLongIn(String name);

    NewRoutineBuilder paramStringIn(String name, int length);

    NewRoutineBuilder paramDoubleIn(String name);
    
    NewRoutineBuilder paramLongOut(String name);

    NewRoutineBuilder paramStringOut(String name, int length);

    NewRoutineBuilder paramDoubleOut(String name);
    
    NewRoutineBuilder externalName(String className);

    NewRoutineBuilder externalName(String className, String methodName);

    NewRoutineBuilder externalName(String jarName, String className, String methodName);

    NewRoutineBuilder externalName(String jarSchema, String jarName, 
                                   String className, String methodName);

    NewRoutineBuilder procDef(String definition);

    NewRoutineBuilder sqlAllowed(SQLAllowed sqlAllowed);

    NewRoutineBuilder dynamicResultSets(int dynamicResultSets);

    NewRoutineBuilder deterministic(boolean deterministic);

    NewRoutineBuilder calledOnNullInput(boolean calledOnNullInput);
}
