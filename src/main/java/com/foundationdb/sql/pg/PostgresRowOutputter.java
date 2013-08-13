/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

import com.foundationdb.qp.row.Row;

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
