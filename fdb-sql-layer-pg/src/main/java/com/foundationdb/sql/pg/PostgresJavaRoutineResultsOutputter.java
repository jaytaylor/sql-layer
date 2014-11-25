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

package com.foundationdb.sql.pg;

import com.foundationdb.ais.model.Parameter;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.sql.server.ServerJavaRoutine;
import com.foundationdb.sql.server.ServerJavaValues;

import java.util.List;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PostgresJavaRoutineResultsOutputter extends PostgresOutputter<ServerJavaRoutine>
{
    public PostgresJavaRoutineResultsOutputter(PostgresQueryContext context,
                                               PostgresJavaRoutine statement) {
        super(context, statement);
    }

    @Override
    public void output(ServerJavaRoutine javaRoutine) throws IOException {
        messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
        messenger.writeShort(ncols);
        int fieldIndex = 0;
        Routine routine = javaRoutine.getInvocation().getRoutine();
        List<Parameter> params = routine.getParameters();
        for (int i = 0; i < params.size(); i++) {
            Parameter param = params.get(i);
            if (param.getDirection() == Parameter.Direction.IN) continue;
            output(javaRoutine, param, i, fieldIndex++);
        }
        if (routine.getReturnValue() != null) {
            output(javaRoutine, routine.getReturnValue(), ServerJavaValues.RETURN_VALUE_INDEX, fieldIndex++);
        }
        messenger.sendMessage();
    }

    protected void output(ServerJavaRoutine javaRoutine, Parameter param, int i, int fieldIndex) throws IOException {
        Object field = javaRoutine.getOutParameter(param, i);
        PostgresType type = columnTypes.get(fieldIndex);
        boolean binary = context.isColumnBinary(fieldIndex);
        ByteArrayOutputStream bytes = encoder.encodePObject(field, type, binary);
        if (bytes == null) {
            messenger.writeInt(-1);
        }
        else {
            messenger.writeInt(bytes.size());
            messenger.writeByteStream(bytes);
        }
    }

}
