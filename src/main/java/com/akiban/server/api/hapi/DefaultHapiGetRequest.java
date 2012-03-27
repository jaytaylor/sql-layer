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

package com.akiban.server.api.hapi;

import com.akiban.ais.model.TableName;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiPredicate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DefaultHapiGetRequest implements HapiGetRequest {

    private final TableName predicatesTable;
    private final String rootTable;
    private final List<HapiPredicate> predicates;
    private final int limit;

    public static HapiGetRequestBuilder forTables(String schema, String rootTable, String predicateTable) {
        return new HapiGetRequestBuilder(schema, rootTable, predicateTable);
    }

    public static HapiGetRequestBuilder forTable(String schema, String rootTable) {
        return new HapiGetRequestBuilder(schema, rootTable, rootTable);
    }

    DefaultHapiGetRequest(String rootTable, TableName predicatesTable, List<HapiPredicate> predicates, int limit) {
        this.predicatesTable = predicatesTable;
        this.rootTable = rootTable;
        this.predicates = Collections.unmodifiableList(new ArrayList<HapiPredicate>(predicates));
        this.limit = limit;
    }


    DefaultHapiGetRequest(String rootTable, TableName predicatesTable, List<HapiPredicate> predicates) {
        this(rootTable, predicatesTable, predicates, -1);
    }

    DefaultHapiGetRequest(String rootTable, TableName predicatesTable, HapiPredicate predicate) {
        this(rootTable, predicatesTable, Arrays.asList(predicate));
    }

    @Override
    public String getSchema() {
        return predicatesTable.getSchemaName();
    }

    @Override
    public String getTable() {
        return rootTable;
    }

    @Override
    public TableName getUsingTable() {
        return predicatesTable;
    }

    @Override
    public List<HapiPredicate> getPredicates() {
        return predicates;
    }

    @Override
    public int getLimit() {
        return limit;
    }

    @Override
    public String toString() {
        return HapiUtils.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof HapiGetRequest && HapiUtils.equals(this, (HapiGetRequest) o);
    }

    @Override
    public int hashCode() {
        return HapiUtils.hashCode(this);
    }
}
