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
import com.akiban.ais.model.UserTable;

import com.akiban.server.error.DuplicateIndexIdException;

import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;

class IndexIDsUnique implements AISValidation {
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        Map<Group, Map<Integer, Index>> byGroup = new HashMap<Group, Map<Integer, Index>>();
        for (UserTable table : ais.getUserTables().values()) {
            for (Index index : table.getIndexesIncludingInternal()) {
                checkIndexID(byGroup, table.getGroup(), index, output);
            }
        }
        for (Group group : ais.getGroups().values()) {
            for (Index index : group.getIndexes()) {
                checkIndexID(byGroup, group, index, output);
            }
        }
    }
    
    private static void checkIndexID(Map<Group, Map<Integer, Index>> byGroup, Group group, Index index, AISValidationOutput failures) {
        Map<Integer, Index> forGroup = byGroup.get(group);
        if (forGroup == null) {
            forGroup = new TreeMap<Integer, Index>();
            byGroup.put(group, forGroup);
        }
        Index other = forGroup.put(index.getIndexId(), index);
        if (other != null) {
            failures.reportFailure(new AISValidationFailure(new DuplicateIndexIdException(other.getIndexName(), index.getIndexName())));
        }
    }
}
