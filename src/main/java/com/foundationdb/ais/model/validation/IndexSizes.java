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

package com.foundationdb.ais.model.validation;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.HKey;
import com.foundationdb.ais.model.HKeyColumn;
import com.foundationdb.ais.model.HKeySegment;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Type;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.server.encoding.EncoderFactory;
import com.foundationdb.server.error.JoinParentNoExplicitPK;
import com.foundationdb.server.error.UnsupportedIndexPrefixException;
import com.foundationdb.server.error.UnsupportedIndexSizeException;

import com.persistit.Key;
import com.persistit.Transaction;

class IndexSizes implements AISValidation {
    /**
     * Maximum size that can can be stored in an index. See
     * {@link Transaction#prepareTxnExchange(com.persistit.Tree, com.persistit.Key, char)}
     * for details on upper bound.
     */
    static final int MAX_INDEX_STORAGE_SIZE = Key.MAX_KEY_LENGTH - 32;

    /**
     * Maximum size for an ordinal value as stored with the HKey. Note that this
     * <b>must match</b> the EWIDTH_XXX definition from {@link Key}, where XXX
     * is the return type of {@link RowDef#getOrdinal()}. Currently this is
     * int and {@link Key#EWIDTH_INT}.
     */
    static final int MAX_ORDINAL_STORAGE_SIZE = 5;

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (UserTable table : ais.getUserTables().values()) {
            long hkeySize = 0;
            if (table.getGroup() != null) {
                hkeySize = validateHKeySize (table, output);
            }

            for(Index index : table.getIndexesIncludingInternal()) {
                long fullKeySize = hkeySize;
                for(IndexColumn iColumn : index.getKeyColumns()) {
                    final Column column = iColumn.getColumn();
                    // Only indexed columns not in hkey contribute new information
                    if (table.getGroup() == null || 
                            !table.hKey().containsColumn(column)) {
                        fullKeySize += getMaxKeyStorageSize((column));
                    }

                    // Reject prefix indexes until supported (bug760202)
                    if(index.isUnique() && iColumn.getIndexedLength() != null) {
                        output.reportFailure(new AISValidationFailure (
                                new UnsupportedIndexPrefixException (table.getName(), index.getIndexName().getName())));
                    }
                }
                if(fullKeySize > MAX_INDEX_STORAGE_SIZE) {
                    output.reportFailure(new AISValidationFailure(
                            new UnsupportedIndexSizeException (table.getName(), index.getIndexName().getName())));
                }
            }
        }
        
        for (Group group : ais.getGroups().values()) {
            for (GroupIndex index : group.getIndexes()) {
                long hkeySize = validateHKeySize(index.leafMostTable(), output);
                long fullKeySize = hkeySize;
                for(IndexColumn iColumn : index.getKeyColumns()) {
                    final Column column = iColumn.getColumn();
                    // Only indexed columns not in hkey contribute new information
                    if (!index.leafMostTable().hKey().containsColumn(column)) {
                        fullKeySize += getMaxKeyStorageSize((column));
                    }

                    // Reject prefix indexes until supported (bug760202)
                    if(index.isUnique() && iColumn.getIndexedLength() != null) {
                        output.reportFailure(new AISValidationFailure (
                                new UnsupportedIndexPrefixException (index.leafMostTable().getName(), 
                                        index.getIndexName().getName())));
                    }
                }
                if (fullKeySize > MAX_INDEX_STORAGE_SIZE) {
                    output.reportFailure(new AISValidationFailure(
                            new UnsupportedIndexSizeException (group.getRoot().getName(), index.getIndexName().getName())));
                }
            }
        }
    }

    private long getMaxKeyStorageSize(final Column column) {
        final Type type = column.getType();
        return EncoderFactory.valueOf(type.encoding(), type).getMaxKeyStorageSize(column);
    }

    private long validateHKeySize (UserTable table, AISValidationOutput output) {
        long hkeySize = 0;
        HKey hkey; 
        try {
            hkey = table.hKey();
        } catch (JoinParentNoExplicitPK ex) {
            // Bug 931258 : 
            // The HKey Calculations on the join assumes the parent 
            // has an explicit Primary Key. The NPE results when the parent table 
            // does not, or the join is to the wrong columns. Both of these 
            // are checked in other validations.
            return hkeySize;
        }
        int ordinalSize = 0;
        for(HKeySegment hkSeg : hkey.segments()) {
            ordinalSize += MAX_ORDINAL_STORAGE_SIZE; // one per segment (i.e. table)
            for(HKeyColumn hkCol : hkSeg.columns()) {
                hkeySize += getMaxKeyStorageSize(hkCol.column());
            }
        }
        // HKey is too large due to pk being too big or group is too nested
        if((hkeySize + ordinalSize) > MAX_INDEX_STORAGE_SIZE) {
            output.reportFailure(new AISValidationFailure (
                    new UnsupportedIndexSizeException (table.getName(), "HKey")));
        }
        return hkeySize;
    }
}
