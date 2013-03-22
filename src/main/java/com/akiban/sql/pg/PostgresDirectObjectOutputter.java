
package com.akiban.sql.pg;

import com.akiban.server.types3.Types3Switch;
import java.util.List;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PostgresDirectObjectOutputter extends PostgresOutputter<List<?>>
{
    public PostgresDirectObjectOutputter(PostgresQueryContext context,
                                         PostgresDMLStatement statement) {
        super(context, statement);
    }

    @Override
    public void output(List<?> row, boolean usePVals) throws IOException {
        messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
        messenger.writeShort(ncols);
        for (int i = 0; i < ncols; i++) {
            Object field = row.get(i);
            PostgresType type = columnTypes.get(i);
            boolean binary = context.isColumnBinary(i);
            ByteArrayOutputStream bytes;
            if (usePVals) bytes = encoder.encodePObject(field, type, binary);
            else bytes = encoder.encodeObject(field, type, binary);
            if (field == null) {
                messenger.writeInt(-1);
            }
            else {
                messenger.writeInt(bytes.size());
                messenger.writeByteStream(bytes);
            }
        }
        messenger.sendMessage();
    }

}
