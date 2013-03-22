
package com.akiban.sql.pg;

import com.akiban.ais.model.Parameter;
import com.akiban.ais.model.Routine;
import com.akiban.sql.server.ServerJavaRoutine;
import com.akiban.sql.server.ServerJavaValues;

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
    public void output(ServerJavaRoutine javaRoutine, boolean usePVals) throws IOException {
        messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
        messenger.writeShort(ncols);
        int fieldIndex = 0;
        Routine routine = javaRoutine.getInvocation().getRoutine();
        List<Parameter> params = routine.getParameters();
        for (int i = 0; i < params.size(); i++) {
            Parameter param = params.get(i);
            if (param.getDirection() == Parameter.Direction.IN) continue;
            output(javaRoutine, param, i, fieldIndex++, usePVals);
        }
        if (routine.getReturnValue() != null) {
            output(javaRoutine, routine.getReturnValue(), ServerJavaValues.RETURN_VALUE_INDEX, fieldIndex++, usePVals);
        }
        messenger.sendMessage();
    }

    protected void output(ServerJavaRoutine javaRoutine, Parameter param, int i, int fieldIndex, boolean usePVals) throws IOException {
        Object field = javaRoutine.getOutParameter(param, i);
        PostgresType type = columnTypes.get(fieldIndex);
        boolean binary = context.isColumnBinary(fieldIndex);
        ByteArrayOutputStream bytes;
        if (usePVals) bytes = encoder.encodePObject(field, type, binary);
        else bytes = encoder.encodeObject(field, type, binary);
        if (bytes == null) {
            messenger.writeInt(-1);
        }
        else {
            messenger.writeInt(bytes.size());
            messenger.writeByteStream(bytes);
        }
    }

}
