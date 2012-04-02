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
import com.akiban.server.service.memcache.SimpleHapiPredicate;
import com.akiban.util.ArgumentValidation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.akiban.server.api.HapiPredicate.Operator.*;

public class HapiGetRequestBuilder {
    private final String rootTable;
    private final TableName predicatesTable;

    HapiGetRequestBuilder(String schema, String rootTable, String predicatesTable) {
        ArgumentValidation.notNull("schema", schema);
        ArgumentValidation.notNull("root table", rootTable);
        ArgumentValidation.notNull("predicates table", predicatesTable);
        this.rootTable = rootTable;
        this.predicatesTable = new TableName(schema, predicatesTable);
    }

    public HapiPredicateBuilder where(String column) {
        return new HapiPredicateBuilder(column);
    }

    public HapiEqualityBuilder withEqualities(String column, String value) {
        HapiEqualityBuilder builder = new HapiEqualityBuilder();
        builder.and(column, value);
        return builder;
    }
    
    public class HapiPredicateBuilder {
        private final String column;

        public HapiPredicateBuilder(String column) {
            this.column = column;
        }

        public HapiGetRequest eq(String value) {
            return new DefaultHapiGetRequest(rootTable, predicatesTable, predicate(EQ, value));
        }

        public HapiGetRequest lt(String value) {
            return new DefaultHapiGetRequest(rootTable, predicatesTable, predicate(LT, value));
        }

        public HapiGetRequest lte(String value) {
            return new DefaultHapiGetRequest(rootTable, predicatesTable, predicate(LTE, value));
        }

        public HapiGetRequest gt(String value) {
            return new DefaultHapiGetRequest(rootTable, predicatesTable, predicate(GT, value));
        }

        public HapiGetRequest gte(String value) {
            return new DefaultHapiGetRequest(rootTable, predicatesTable, predicate(GTE, value));
        }

        public HapiRangeBuilder between(String bottomValue, String topValue) {
            return new HapiRangeBuilder(this, bottomValue, topValue);
        }

        public HapiPredicate predicate(HapiPredicate.Operator operator, String value) {
            return new SimpleHapiPredicate(predicatesTable, column, operator, value);
        }
    }

    public class HapiRangeBuilder {
        private final HapiPredicateBuilder predicateBuilder;
        private final String bottomValue;
        private final String topValue;

        public HapiRangeBuilder(HapiPredicateBuilder predicateBuilder, String bottomValue, String topValue) {
            this.predicateBuilder = predicateBuilder;
            this.bottomValue = bottomValue;
            this.topValue = topValue;
        }

        public HapiGetRequest inclusive() {
            return build(GTE, LTE);
        }

        public HapiGetRequest exclusive() {
            return build(GT, LT);
        }

        public HapiGetRequest includeBottomExcludeTop() {
            return build(GTE, LT);
        }

        public HapiGetRequest excludeBottomIncludeTop() {
            return build(GT, LTE);
        }

        private HapiGetRequest build(HapiPredicate.Operator bottom, HapiPredicate.Operator top) {
            HapiPredicate[] predicates = new HapiPredicate[2];
            predicates[0] = predicateBuilder.predicate(bottom, bottomValue);
            predicates[1] = predicateBuilder.predicate(top, topValue);
            return new DefaultHapiGetRequest(rootTable, predicatesTable, Arrays.asList(predicates));
        }
    }

    public class HapiEqualityBuilder {
        private final Set<String> columns = new HashSet<String>();
        private final List<HapiPredicate> predicates = new ArrayList<HapiPredicate>();

        public HapiEqualityBuilder and(String column, String value) {
            if (!columns.add(column)) {
                throw new IllegalStateException("already defined value for " + column);
            }
            predicates.add( new SimpleHapiPredicate(predicatesTable, column, EQ, value));
            return this;
        }

        public HapiGetRequest done() {
            return new DefaultHapiGetRequest(rootTable, predicatesTable, predicates);
        }
    }
}
