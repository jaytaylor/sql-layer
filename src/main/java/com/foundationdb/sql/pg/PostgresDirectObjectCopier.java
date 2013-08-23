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
    public void output(List<?> row) throws IOException {
        messenger.beginMessage(PostgresMessages.COPY_DATA_TYPE.code());
        encoder.reset();
        for (int i = 0; i < ncols; i++) {
            if (i > 0) encoder.appendString("|");
            Object field = row.get(i);
            PostgresType type = columnTypes.get(i);
            if (field != null)
                encoder.appendPObject(field, type, false);
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
