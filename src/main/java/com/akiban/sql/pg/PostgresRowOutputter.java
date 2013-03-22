
package com.akiban.sql.pg;

import com.akiban.qp.row.Row;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PostgresRowOutputter extends PostgresOutputter<Row>
{
    public PostgresRowOutputter(PostgresQueryContext context,
                                PostgresDMLStatement statement) {
        super(context, statement);
    }

    @Override
    public void output(Row row, boolean usePVals) throws IOException {
        messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
        messenger.writeShort(ncols);
        for (int i = 0; i < ncols; i++) {
            PostgresType type = columnTypes.get(i);
            boolean binary = context.isColumnBinary(i);
            ByteArrayOutputStream bytes;
            if (usePVals) bytes = encoder.encodePValue(row.pvalue(i), type, binary);
            else bytes = encoder.encodeValue(row.eval(i), type, binary);
            if (bytes == null) {
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
