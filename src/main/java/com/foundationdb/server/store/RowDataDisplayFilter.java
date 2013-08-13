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

package com.akiban.server.store;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.protobuf.AISProtobuf;
import com.akiban.ais.protobuf.ProtobufReader;
import com.akiban.server.service.tree.TreeService;
import com.akiban.util.GrowableByteBuffer;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Management.DisplayFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RowDataDisplayFilter implements DisplayFilter {
    private static final Logger LOG = LoggerFactory.getLogger(RowDataDisplayFilter.class.getName());

    private DisplayFilter defaultFilter;

    public RowDataDisplayFilter(final DisplayFilter filter) {
        this.defaultFilter = filter;
    }

    public String toKeyDisplayString(final Exchange exchange) {
        return defaultFilter.toKeyDisplayString(exchange);
    }

    public String toValueDisplayString(final Exchange exchange) {
        final String treeName = exchange.getTree().getName();

        if (treeName.equals(TreeService.SCHEMA_TREE_NAME)) {
            try {
                final Key key = exchange.getKey();
                if(key.decodeString().equals("byPBAIS")) {
                    // Default string, will include MVV information
                    String def = defaultFilter.toValueDisplayString(exchange);
                    // New exchange (that doesn't have ignore MVV set), to get pretty-print of latest
                    Exchange ex = new Exchange(exchange);
                    byte[] storedAIS = ex.fetch().getValue().getByteArray();
                    GrowableByteBuffer buffer = GrowableByteBuffer.wrap(storedAIS);
                    AkibanInformationSchema ais = new AkibanInformationSchema();
                    AISProtobuf.AkibanInformationSchema.Builder pbAIS = AISProtobuf.AkibanInformationSchema.newBuilder();
                    new ProtobufReader(ais, pbAIS).loadBuffer(buffer);
                    return "raw:\n" + def + "\nlatest:\n" + pbAIS.build().toString();
                }
            } catch (Exception e) {
                LOG.debug("Failure to decode byPBAIS key {}", e);
            }
        }
        return defaultFilter.toValueDisplayString(exchange);
    }
}
