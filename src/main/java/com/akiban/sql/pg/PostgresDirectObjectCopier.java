
package com.akiban.sql.pg;

import java.util.List;
import java.io.IOException;

/** Output object rows using the COPY protocol, which has the
 * advantage of being asynchronous. Useful when the loadable plan does
 * something that takes a while and produces output as it goes.
 */
public class PostgresDirectObjectCopier extends PostgresOutputter<List<?>>
{
    private boolean withNewline;

    public PostgresDirectObjectCopier(PostgresQueryContext context,
                                      PostgresDMLStatement statement,
                                      boolean withNewline) {
        super(context, statement);
        this.withNewline = withNewline;
    }

    @Override
    public void output(List<?> row, boolean usePVals) throws IOException {
        messenger.beginMessage(PostgresMessages.COPY_DATA_TYPE.code());
        encoder.reset();
        for (int i = 0; i < ncols; i++) {
            if (i > 0) encoder.appendString("|");
            Object field = row.get(i);
            PostgresType type = columnTypes.get(i);
            if (field != null)
                encoder.appendObject(field, type, false);
        }
        if (withNewline)
            encoder.appendString("\n");
        messenger.writeByteStream(encoder.getByteStream());
        messenger.sendMessage();
    }

    public void respond() throws IOException {
        messenger.beginMessage(PostgresMessages.COPY_OUT_RESPONSE_TYPE.code());
        messenger.write(0);
        messenger.writeShort(ncols);
        for (int i = 0; i < ncols; i++) {
            assert !context.isColumnBinary(i);
            messenger.writeShort(0);
        }
        messenger.sendMessage();
    }

    public void done() throws IOException {
        messenger.beginMessage(PostgresMessages.COPY_DONE_TYPE.code());
        messenger.sendMessage();
    }

}
