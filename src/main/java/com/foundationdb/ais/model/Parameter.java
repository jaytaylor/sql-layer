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
import com.foundationdb.server.types.TInstance;

import java.util.UUID;

public class Parameter
{
    public static enum Direction { IN, OUT, INOUT, RETURN };

    public static Parameter create(Routine routine, String name, Direction direction,
                                   TInstance type)
    {
        routine.checkMutability();
        if (name != null)
            AISInvariants.checkDuplicateParametersInRoutine(routine, name, direction);
        Parameter parameter = new Parameter(routine, name, direction, type);
        routine.addParameter(parameter);
        return parameter;
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

    public TInstance getType() {
        return type;
    }

    public String getTypeName() {
        return type.typeClass().name().unqualifiedName();
    }

    public UUID getTypeBundleUUID() {
        return type.typeClass().name().bundleId().uuid();
    }

    public int getTypeVersion() {
        return type.typeClass().serializationVersion();
    }

    public String getTypeDescription()
    {
        return type.toStringConcise(true);
    }

    public Long getTypeParameter1()
    {
        return Column.getTypeParameter1(type);
    }

    public Long getTypeParameter2()
    {
        return Column.getTypeParameter2(type);
    }

    private Parameter(Routine routine,
                      String name,
                      Direction direction,
                      TInstance type)
    {
        this.routine = routine;
        this.name = name;
        this.direction = direction;
        this.type = type;
    }

    // State

    private final Routine routine;
    private final String name;
    private final Direction direction;
    private final TInstance type;
}
