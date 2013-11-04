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

package com.foundationdb.server.store.format.protobuf;

import com.google.protobuf.DynamicMessage;

/** This holder object is necessary for {@link ProtobufValueCoder} to
 * work straightforwardly.
 */
public class PersistitProtobufRow
{
    private final ProtobufRowDataConverter converter;
    private DynamicMessage msg;

    public PersistitProtobufRow(ProtobufRowDataConverter converter, DynamicMessage msg) {
        this.converter = converter;
        this.msg = msg;
    }

    public ProtobufRowDataConverter getConverter() {
        return converter;
    }

    public DynamicMessage getMessage() {
        return msg;
    }
    protected void setMessage(DynamicMessage msg) {
        this.msg = msg;
    }

    @Override
    public String toString() {
        return converter.shortFormat(msg);
    }
}
