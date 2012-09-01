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
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.IndexTreeNameIsNullException;
import com.akiban.server.error.SequenceTreeNameIsNullException;
import com.akiban.server.error.TableTreeNameIsNullException;

import java.util.Collection;

/**
 * Check all table and index tree names are not null.
 */
public class TreeNamesAreNotNull implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for(UserTable table : ais.getUserTables().values()) {
            checkTable(table);
        }
        for(Group group : ais.getGroups().values()) {
            for(Index index : group.getIndexes()) {
                checkIndex(index);
            }
        }
        for (Sequence sequence: ais.getSequences().values()) {
            checkSequence(sequence);
        }
    }

    private static void checkTable(Table table) {
        if(table.getTreeName() == null) {
            throw new TableTreeNameIsNullException(table);
        }
        final Collection<TableIndex> indexes;
        if(table.isUserTable()) {
            indexes = ((UserTable)table).getIndexesIncludingInternal();
        } else {
            indexes = table.getIndexes();
        }
        for(Index index : indexes) {
            checkIndex(index);
        }
    }

    private static void checkIndex(Index index) {
        if(index.getTreeName() == null) {
            throw new IndexTreeNameIsNullException(index);
        }
    }
    
    private static void checkSequence(Sequence sequence) {
        if (sequence.getTreeName() == null) {
            throw new SequenceTreeNameIsNullException(sequence);
        }
    }
}
