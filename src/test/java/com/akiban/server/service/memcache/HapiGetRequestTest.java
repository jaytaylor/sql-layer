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

package com.akiban.server.service.memcache;

import com.akiban.ais.model.TableName;
import com.akiban.junit.OnlyIf;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiPredicate;
import com.akiban.server.api.HapiRequestException;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;

import static com.akiban.server.api.HapiPredicate.Operator.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class HapiGetRequestTest {

    private static class HapiGetRequestBuilder {
        private final ParsedHapiGetRequest request;

        HapiGetRequestBuilder(ParsedHapiGetRequest request) {
            this.request = request;
        }

        HapiGetRequestBuilder predicate(
                String columnName, SimpleHapiPredicate.Operator operator, String value
       ) {
            int oldsize = request.getPredicates().size();
            request.addPredicate(columnName, operator, value);
            assertEquals("predicates.size()", oldsize+1, request.getPredicates().size());

            HapiPredicate actual = request.getPredicates().get(oldsize);
            SimpleHapiPredicate expected
                    = new SimpleHapiPredicate(request.getUsingTable(), columnName, operator, value);

            assertEquals("predicate", expected, actual);
            assertEquals("predicate hash", expected.hashCode(), actual.hashCode());

            assertEquals("predicate table", request.getUsingTable(), actual.getTableName());
            assertEquals("predicate column", columnName, actual.getColumnName());
            assertEquals("predicate op", operator, actual.getOp());
            assertEquals("predicate value", value, actual.getValue());
            return this;
        }

        HapiGetRequestBuilder limit(int limit) {
            assert request.getLimit() < 0;
            request.setLimit(Integer.toString(limit));
            return this;
        }
    }

    private static class Parameterizations {
        private final ParameterizationBuilder params = new ParameterizationBuilder();

        void addFailing(String queryString) {
            params.add(queryString, queryString, null);
//            params.addFailing(queryString, queryString, null);
        }

        HapiGetRequestBuilder add(String queryString, String schema, String table, String usingTable) {
            ParsedHapiGetRequest request = new ParsedHapiGetRequest();
            request.setSchema(schema);
            request.setTable(table);
            request.setUsingTable(usingTable);


            assertEquals(queryString + " schema", schema, request.getSchema());
            assertEquals(queryString + " table", table, request.getTable());
            assertEquals(queryString + " usingTable", new TableName(schema, usingTable), request.getUsingTable());

            params.add(queryString, queryString, request);
            return new HapiGetRequestBuilder(request);
        }

        public List<Parameterization> getParams() {
            return params.asList();
        }
    }

    @NamedParameterizedRunner.TestParameters
    public static List<Parameterization> data() {
        Parameterizations params = new Parameterizations();

        params.add("coi:customer:cid=5", "coi", "customer", "customer").predicate("cid", EQ, "5");
        params.add("coi:customer:name='snowman'", "coi", "customer", "customer").predicate("name", EQ, "snowman");
        params.add("coi:customer:name='NULL'", "coi", "customer", "customer").predicate("name", EQ, "NULL");
        params.add("coi:customer:name=null", "coi", "customer", "customer").predicate("name", EQ, null);
        params.add("coi:customer:name=NULL", "coi", "customer", "customer").predicate("name", EQ, null);
        params.add("coi:customer:name=''", "coi", "customer", "customer").predicate("name", EQ, "");
        params.add(".-*_:c_table:c_name=a_b", ".-*_", "c_table", "c_table").predicate("c_name", EQ, "a_b");

        params.add("coi:customer:(order)description=snowglobe", "coi", "customer", "order")
                .predicate("description", EQ, "snowglobe");
        params.add("coi:customer:(order)description=snowglobe,color!="+escape("whité"), "coi", "customer", "order")
                .predicate("description", EQ, "snowglobe")
                .predicate("color", NE, "whit\u00e9"); // whité is a fancy, French shade of white
        params.add("coi:customer:name="+escape("☃"), "coi", "customer", "customer").predicate("name", EQ, "\u2603");

        String weirdQuery = String.format("%s:%s:(%s)%s=%s",
                escape("☺"), escape("♪"), escape("☠"), escape("★"), escape("☘"));
        params.add(weirdQuery, "☺", "♪", "☠").predicate("★", EQ, "☘");

        params.add("coi:customer:(:LIMIT=2)name=bob", "coi", "customer", "customer")
                .predicate("name", EQ, "bob")
                .limit(2);
        params.add("coi:customer:(order:LIMIT=2)name=bob", "coi", "customer", "order")
                .predicate("name", EQ, "bob")
                .limit(2);
        params.add("coi:customer:restriction=limit", "coi", "customer", "customer")
                .predicate("restriction", EQ, "limit");
        params.add("coi:customer:restriction='LIMIT'", "coi", "customer", "customer")
                .predicate("restriction", EQ, "LIMIT");
        params.addFailing("coi:customer:name=LIMIT");
        params.addFailing("coi:customer:(LIMIT=2)c=v");
        params.addFailing("coi:customer:(:LIMIT=two)c=v");

        params.addFailing("coi:customer:name='☃'");
        params.addFailing("coi:customer:name=");

        return params.getParams();
    }

    private static String escape(String s) {
        try {
            return '\'' + URLEncoder.encode(s, "UTF-8") + '\'';
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private final String query;
    private final ParsedHapiGetRequest expectedRequest;

    public HapiGetRequestTest(String query, ParsedHapiGetRequest request) {
        this.query = query;
        this.expectedRequest = request;
    }

    public boolean expectedToWork() {
        return expectedRequest != null;
    }

    public boolean expectedToFail() {
        return ! expectedToWork();
    }

    @Test @OnlyIf("expectedToFail")
    public void fails() {
        Exception exception = null;
        try {
            HapiGetRequest request = ParsedHapiGetRequest.parse(query);
            assert false : request;
        } catch (Exception e) {
            exception = e;
        }
        assertNotNull(query + "expected exception", exception);
    }

    @Test @OnlyIf("expectedToWork")
    public void equality() throws  HapiRequestException {
        HapiGetRequest actual = ParsedHapiGetRequest.parse(query);
        assertEquals(query + " request", expectedRequest, actual);
    }

    @Test @OnlyIf("expectedToWork")
    public void hash() throws  HapiRequestException {
        HapiGetRequest actual = ParsedHapiGetRequest.parse(query);
        assertEquals(query + " hash", expectedRequest.hashCode(), actual.hashCode());
    }

    @Test @OnlyIf("expectedToWork")
    public void schema() throws  HapiRequestException {
        HapiGetRequest actual = ParsedHapiGetRequest.parse(query);
        assertEquals(query + " schema", expectedRequest.getSchema(), actual.getSchema());
    }

    @Test @OnlyIf("expectedToWork")
    public void table() throws  HapiRequestException {
        HapiGetRequest actual = ParsedHapiGetRequest.parse(query);
        assertEquals(query + " table", expectedRequest.getTable(), actual.getTable());
    }

    @Test @OnlyIf("expectedToWork")
    public void predicateTable() throws  HapiRequestException {
        HapiGetRequest actual = ParsedHapiGetRequest.parse(query);
        assertEquals(query + " usingtable", expectedRequest.getUsingTable(), actual.getUsingTable());
    }

    @Test @OnlyIf("expectedToWork")
    public void predicates() throws  HapiRequestException {
        HapiGetRequest actual = ParsedHapiGetRequest.parse(query);
        assertEquals(query + " predicates", expectedRequest.getPredicates(), actual.getPredicates());
    }
}
