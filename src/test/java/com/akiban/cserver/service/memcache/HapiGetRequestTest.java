package com.akiban.cserver.service.memcache;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import static com.akiban.cserver.service.memcache.HapiGetRequest.Predicate.Operator.*;
import static org.junit.Assert.*;

@RunWith(org.junit.runners.Parameterized.class)
// TODO Migrate to Yuval's named-parameterized runner as soon as it's available
public final class HapiGetRequestTest {

    private static class HapiGetRequestBuilder {
        private final HapiGetRequest request;

        HapiGetRequestBuilder(HapiGetRequest request) {
            this.request = request;
        }

        HapiGetRequestBuilder predicate(
                String columnName, HapiGetRequest.Predicate.Operator operator, String value
       ) {
            request.addPredicate(columnName, operator, value);
            return this;
        }
    }

    private static class Parameterizations {
        private final List<Object[]> params = new ArrayList<Object[]>();

        void addFailing(String queryString) {
            params.add( new Object[] {queryString, null} );
        }

        HapiGetRequestBuilder add(String queryString, String schema, String table, String usingTable) {
            HapiGetRequest request = new HapiGetRequest();
            request.setSchema(schema);
            request.setTable(table);
            request.setUsingTable(usingTable);
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
    private final HapiGetRequest expectedRequest;


    private final static HapiGetRequest.ParseErrorReporter ERROR_REPORTER = new HapiGetRequest.ParseErrorReporter() {
        @Override
        public void reportError(String error) {
            throw new RuntimeException(error);
        }
    };

    public HapiGetRequestTest(String query, HapiGetRequest request) {
        this.query = query;
        this.expectedRequest = request;
    }

    @Test
    public void test() {
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
            HapiGetRequest request = HapiGetRequest.parse(query, ERROR_REPORTER);
            assert false : request;
        } catch (Exception e) {
            exception = e;
        }
        assertNotNull(query + "expected exception", exception);
    }

    private void testWorking() {
        HapiGetRequest actual = HapiGetRequest.parse(query, ERROR_REPORTER);
        assertEquals(query + "request", expectedRequest, actual);
    }
}
