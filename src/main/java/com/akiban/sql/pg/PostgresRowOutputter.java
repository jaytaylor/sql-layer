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

import java.io.IOException;

public class PostgresRowOutputter extends PostgresOutputter<Row>
{
    private ToObjectValueTarget target;

    public PostgresRowOutputter(PostgresMessenger messenger, 
                                PostgresBaseStatement statement) {
        super(messenger, statement);
        target = new ToObjectValueTarget();
    }

    @Override
    public void output(Row row) throws IOException {
        messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
        messenger.writeShort(ncols);
        for (int i = 0; i < ncols; i++) {
            Object field = target.convertFromSource(row.eval(i));
            PostgresType type = columnTypes.get(i);
            byte[] value = type.encodeValue(field,
                                            messenger.getEncoding(),
                                            statement.isColumnBinary(i));
            if (value == null) {
                messenger.writeInt(-1);
            }
            else {
                messenger.writeInt(value.length);
                messenger.write(value);
            }
        }
        messenger.sendMessage();
    }

}
