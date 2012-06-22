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

package com.akiban.server.test.it.dxl;

import com.akiban.ais.model.*;
import com.akiban.ais.model.validation.AISValidation;
import com.akiban.ais.model.validation.AISValidationFailure;
import com.akiban.ais.model.validation.AISValidationOutput;
import com.akiban.ais.model.validation.AISValidationResults;
import com.akiban.server.error.DuplicateIndexTreeNamesException;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

// Inspired by bug 873070

public class DuplicateIndexTreeNameIT extends ITBase
{
    @Test
    public void createRenameCreate()
    {
        createTable("schema", "root", "id int not null, primary key(id)");
        createTable("schema", "child", "id int not null, rid int, primary key(id)", akibanFK("rid", "root", "id"));
        ddl().renameTable(session(), tableName("schema", "child"), tableName("schema", "renamed_child"));
        createTable("schema", "child", "id int not null, rid int, primary key(id)", akibanFK("rid", "root", "id"));
        AkibanInformationSchema ais = ddl().getAIS(session());
        AISValidationResults results = ais.validate(Collections.singleton((AISValidation)new  IndexTreeNamesUnique()));
        assertEquals(0, results.failures().size());
    }

    // Copy of class in com.akiban.ais.model.validation, but it isn't public.
    static class IndexTreeNamesUnique implements AISValidation
    {

        @Override
        public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
            Map<String,Index> treeNameMap = new HashMap<String, Index>();

            for(UserTable table : ais.getUserTables().values()) {
                checkIndexes(output, treeNameMap, table.getIndexes());
            }

            for(Group group : ais.getGroups().values()) {
                checkIndexes(output, treeNameMap, group.getIndexes());
            }
        }

        private static void checkIndexes(AISValidationOutput output, Map<String, Index> treeNameMap,
                                         Collection<? extends Index> indexes) {
            for(Index index : indexes) {
                String treeName = index.getTreeName();
                Index curIndex = treeNameMap.get(treeName);
                if(curIndex != null) {
                    output.reportFailure(
                            new AISValidationFailure(
                                    new DuplicateIndexTreeNamesException(index.getIndexName(), curIndex.getIndexName(), treeName)));
                } else {
                    treeNameMap.put(treeName, index);
                }
            }
        }
    }
}
