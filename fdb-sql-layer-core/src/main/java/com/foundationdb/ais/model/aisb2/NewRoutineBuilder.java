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

package com.foundationdb.ais.model.aisb2;

import static com.foundationdb.ais.model.Routine.*;

public interface NewRoutineBuilder {
    NewRoutineBuilder language(String language, CallingConvention callingConvention);

    NewRoutineBuilder returnBoolean(String name);

    NewRoutineBuilder returnLong(String name);

    NewRoutineBuilder returnString(String name, int length);

    NewRoutineBuilder paramBooleanIn(String name);

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
