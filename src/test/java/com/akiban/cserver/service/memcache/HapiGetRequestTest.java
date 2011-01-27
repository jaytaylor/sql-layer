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

package com.akiban.cserver.service.memcache;

import com.akiban.ais.model.TableName;
import com.akiban.cserver.api.HapiGetRequest;
import com.akiban.cserver.api.HapiRequestException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import static com.akiban.cserver.api.HapiGetRequest.Predicate.Operator.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

@RunWith(org.junit.runners.Parameterized.class)
// TODO Migrate to Yuval's named-parameterized runner as soon as it's available
public final class HapiGetRequestTest {

    private static class HapiGetRequestBuilder {
        private final ParsedHapiGetRequest request;

        HapiGetRequestBuilder(ParsedHapiGetRequest request) {
            this.request = request;
        }

        HapiGetRequestBuilder predicate(
                String columnName, SimplePredicate.Operator operator, String value
       ) {
            int oldsize = request.getPredicates().size();
            request.addPredicate(columnName, operator, value);
            assertEquals("predicates.size()", oldsize+1, request.getPredicates().size());

            HapiGetRequest.Predicate actual = request.getPredicates().get(oldsize);
            SimplePredicate expected
                    = new SimplePredicate(request.getUsingTable(), columnName, operator, value);

            assertEquals("predicate", expected, actual);
            assertEquals("predicate hash", expected.hashCode(), actual.hashCode());

            assertEquals("predicate table", request.getUsingTable(), actual.getTableName());
            assertEquals("predicate column", columnName, actual.getColumnName());
            assertEquals("predicate op", operator, actual.getOp());
            assertEquals("predicate value", value, actual.getValue());
            return this;
        }
    }

    private static class Parameterizations {
        private final List<Object[]> params = new ArrayList<Object[]>();

        void addFailing(String queryString) {
            params.add( new Object[] {queryString, null} );
        }

        HapiGetRequestBuilder add(String queryString, String schema, String table, String usingTable) {
            ParsedHapiGetRequest request = new ParsedHapiGetRequest();
            request.setSchema(schema);
            request.setTable(table);
            request.setUsingTable(usingTable);


            assertEquals(queryString + " schema", schema, request.getSchema());
            assertEquals(queryString + " table", table, request.getTable());
            assertEquals(queryString + " usingTable", new TableName(schema, usingTable), request.getUsingTable());

            params.add( new Object[] {queryString, request} );
            return new HapiGetRequestBuilder(request);
        }

        public List<Object[]> getParams() {
            return params;
        }
    }

    @Parameterized.Parameters
    public static List<Object[]> data() {
        Parameterizations params = new Parameterizations();

        params.add("coi:customer:cid=5", "coi", "customer", "customer").predicate("cid", EQ, "5");
        params.add("coi:customer:name='snowman'", "coi", "customer", "customer").predicate("name", EQ, "snowman");
        params.add("coi:customer:name='NULL'", "coi", "customer", "customer").predicate("name", EQ, "NULL");
        params.add("coi:customer:name=null", "coi", "customer", "customer").predicate("name", EQ, null);
        params.add("coi:customer:name=NULL", "coi", "customer", "customer").predicate("name", EQ, null);
        params.add("coi:customer:name=''", "coi", "customer", "customer").predicate("name", EQ, "");

        params.add("coi:customer:(order)description=snowglobe", "coi", "customer", "order")
                .predicate("description", EQ, "snowglobe");
        params.add("coi:customer:(order)description=snowglobe,color!="+escape("whité"), "coi", "customer", "order")
                .predicate("description", EQ, "snowglobe")
                .predicate("color", NE, "whit\u00e9"); // whité is a fancy, French shade of white
        params.add("coi:customer:name="+escape("☃"), "coi", "customer", "customer").predicate("name", EQ, "\u2603");

        String weirdQuery = String.format("%s:%s:(%s)%s=%s",
                escape("☺"), escape("♪"), escape("☠"), escape("★"), escape("☘"));
        params.add(weirdQuery, "☺", "♪", "☠").predicate("★", EQ, "☘");

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

    @Test
    public void test() throws HapiRequestException {
        if (expectedRequest == null) {
            testFailing();
        }
        else {
            testWorking();
        }
    }

    private void testFailing() {
        Exception exception = null;
        try {
            HapiGetRequest request = ParsedHapiGetRequest.parse(query);
            assert false : request;
        } catch (Exception e) {
            exception = e;
        }
        assertNotNull(query + "expected exception", exception);
    }

    private void testWorking() throws HapiRequestException {
        HapiGetRequest actual = ParsedHapiGetRequest.parse(query);
        assertEquals(query + " request", expectedRequest, actual);
        assertEquals(query + " hash", expectedRequest.hashCode(), actual.hashCode());

        assertEquals(query + " schema", expectedRequest.getSchema(), actual.getSchema());
        assertEquals(query + " table", expectedRequest.getTable(), actual.getTable());
        assertEquals(query + " usingtable", expectedRequest.getUsingTable(), actual.getUsingTable());
        assertEquals(query + " predicates", expectedRequest.getPredicates(), actual.getPredicates());
    }
}
