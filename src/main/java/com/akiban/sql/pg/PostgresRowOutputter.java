/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.pg;

import com.akiban.qp.row.Row;
import com.akiban.server.types.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PostgresRowOutputter extends PostgresOutputter<Row>
{
    public PostgresRowOutputter(PostgresMessenger messenger, 
                                PostgresBaseStatement statement) {
        super(messenger, statement);
    }

    @Override
    public void output(Row row) throws IOException {
        messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
        messenger.writeShort(ncols);
        for (int i = 0; i < ncols; i++) {
            ValueSource field = row.eval(i);
            PostgresType type = columnTypes.get(i);
            boolean binary = statement.isColumnBinary(i);
            ByteArrayOutputStream bytes = encoder.encodeValue(field, type, binary);
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
