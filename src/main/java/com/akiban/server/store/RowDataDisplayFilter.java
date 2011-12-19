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

import com.akiban.ais.io.CSVTarget;
import com.akiban.ais.io.MessageSource;
import com.akiban.ais.io.Reader;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.server.AkServerUtil;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.tree.TreeService;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Management.DisplayFilter;
import com.persistit.Value;

import java.nio.ByteBuffer;

class RowDataDisplayFilter implements DisplayFilter {

    private final static String[] PROTECTED_VOLUME_NAMES = { "akiban_system" };
    private final static String[] PROTECTED_TREE_NAMES = { TreeService.SCHEMA_TREE_NAME };
    private final PersistitStore persistitStore;
    private final TreeService treeService;
    private DisplayFilter defaultFilter;

    public RowDataDisplayFilter(PersistitStore store, TreeService treeService,
            final DisplayFilter filter) {
        this.persistitStore = store;
        this.treeService = treeService;
        this.defaultFilter = filter;
    }

    public String toKeyDisplayString(final Exchange exchange) {
        return defaultFilter.toKeyDisplayString(exchange);
    }

    public String toValueDisplayString(final Exchange exchange) {
        final String treeName = exchange.getTree().getName();
        final String volumeName = exchange.getVolume().getName();
        boolean protectedTree = containsExactlyOne(treeName, "$$");
        if (!protectedTree) {
            for (final String s : PROTECTED_VOLUME_NAMES) {
                if (volumeName.equals(s)) {
                    protectedTree = true;
                    break;
                }
            }
        }
        if (!protectedTree) {
            for (final String s : PROTECTED_TREE_NAMES) {
                if (treeName.equals(s)) {
                    protectedTree = true;
                    break;
                }
            }
        }
        try {
            if (!protectedTree) {
                final Value value = exchange.getValue();
                int rowDefId = AkServerUtil.getInt(value.getEncodedBytes(),
                                                   RowData.O_ROW_DEF_ID - RowData.LEFT_ENVELOPE_SIZE);
                rowDefId = treeService.storeToAis(exchange.getVolume(), rowDefId);
                final RowDef rowDef = persistitStore.getRowDefCache().getRowDef(rowDefId);
                final int size = value.getEncodedSize() + RowData.ENVELOPE_SIZE;
                final byte[] bytes = new byte[size];
                final RowData rowData = new RowData(bytes);
                persistitStore.expandRowData(exchange, rowData);
                return rowData.toString(rowDef);
            }
            else if(treeName.equals(TreeService.SCHEMA_TREE_NAME)) {
                final Key key = exchange.getKey();
                if(key.decodeString().equals("byAIS")) {
                    byte[] storedAIS = exchange.fetch().getValue().getByteArray();
                    ByteBuffer buffer = ByteBuffer.wrap(storedAIS);
                    AkibanInformationSchema ais = new Reader(new MessageSource(buffer)).load();
                    return CSVTarget.toString(ais);
                }
            }
        } catch (Exception e) {
            // fall through
        }
        return defaultFilter.toValueDisplayString(exchange);

    }

    private boolean containsExactlyOne(String haystack, String needle) {
        int first = haystack.indexOf(needle);
        if (first < 0)
            return false;
        return haystack.indexOf(needle, first) < 0;
    }
}