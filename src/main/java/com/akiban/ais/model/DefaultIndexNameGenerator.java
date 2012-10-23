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

package com.akiban.ais.model;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class DefaultIndexNameGenerator implements IndexNameGenerator {
    private final Set<String> indexNames = new HashSet<String>();

    public DefaultIndexNameGenerator(Collection<String> initialIndexNames) {
        indexNames.addAll(initialIndexNames);
    }

    public static DefaultIndexNameGenerator forTable(UserTable table) {
        Set<String> indexNames = new HashSet<String>();
        for(Index index : table.getIndexesIncludingInternal()) {
            indexNames.add(index.getIndexName().getName());
        }
        return new DefaultIndexNameGenerator(indexNames);
    }

    @Override
    public String generateIndexName(String indexName, String columnName, String constraint) {
        if((indexName != null) && !indexNames.contains(indexName)) {
            indexNames.add(indexName);
            return indexName;
        }
        String name = columnName;
        for(int suffixNum = 2; indexNames.contains(name); ++suffixNum) {
            name = String.format("%s_%d", columnName, suffixNum);
        }
        indexNames.add(name);
        return name;
    }
}
