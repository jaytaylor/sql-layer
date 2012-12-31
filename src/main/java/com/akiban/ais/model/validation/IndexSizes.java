/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.HKey;
import com.akiban.ais.model.HKeyColumn;
import com.akiban.ais.model.HKeySegment;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.UserTable;
import com.akiban.server.encoding.EncoderFactory;
import com.akiban.server.error.JoinParentNoExplicitPK;
import com.akiban.server.error.UnsupportedIndexPrefixException;
import com.akiban.server.error.UnsupportedIndexSizeException;

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
