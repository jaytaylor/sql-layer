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

package com.akiban.server.store;

import java.nio.ByteBuffer;

import com.akiban.ais.metamodel.io.CSVTarget;
import com.akiban.ais.metamodel.io.MessageSource;
import com.akiban.ais.metamodel.io.Reader;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.server.service.tree.TreeService;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Management.DisplayFilter;

class RowDataDisplayFilter implements DisplayFilter {
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
                if(key.decodeString().equals("byAIS")) {
                    byte[] storedAIS = exchange.fetch().getValue().getByteArray();
                    ByteBuffer buffer = ByteBuffer.wrap(storedAIS);
                    AkibanInformationSchema ais = new Reader(new MessageSource(buffer)).load();
                    return CSVTarget.toString(ais);
                }
            } catch (Exception e) {
                // fall through and attempt to use default display filter
            }
        }
        return defaultFilter.toValueDisplayString(exchange);
    }
}
