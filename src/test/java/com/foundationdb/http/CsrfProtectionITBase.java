/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.http;

import com.foundationdb.rest.RestService;
import com.foundationdb.rest.RestServiceImpl;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.ITBase;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.containsString;

/**
 *
 * Simple Request:
 *     Method: GET, POST, HEAD
 *     Headers: Origin = foo
 *              [other "simple" headers]
 * Simple Response:
 *     Headers: Access-Control-Allow-Origin = foo
 *              Access-Control-Allow-Credentials = true (if enabled)
 *
 * Non-Simple: Simple + other methods, headers
 *
 * PreFlight Request:
 *     Method: OPTIONS
 *     Headers: Origin = foo
 *              Access-Control-Request-Method = GET
 *              Note: No auth
 * PreFlight Response:
 *     Headers: Access-Control-Allow-Origin = foo
 *              Access-Control-Allow-Methods = ...
 *              Access-Control-Allow-Headers = ...
 */
public abstract class CsrfProtectionITBase extends ITBase
{
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";
    private static final String ORIGIN = "http://example.com";
    private static final String ALLOWED_METHODS = "GET,POST,PUT";
    private static final String HEADER_ALLOW_ORIGIN = "Access-Control-Allow-Origin";


    private int port;
    private String restContext;
    private HttpResponse response;
    private CloseableHttpClient client;

    @Before
    public final void setUp() {
        port = serviceManager().getServiceByClass(HttpConductor.class).getPort();
        restContext = serviceManager().getServiceByClass(RestService.class).getContextPath();
        createTable(SCHEMA, TABLE, "id int not null primary key");
        client = HttpClientBuilder.create().build();
    }

    @After
    public final void tearDown() throws IOException {
        if(response != null) {
            EntityUtils.consume(response.getEntity());
        }
        if(client != null) {
            client.close();
        }
    }


    protected abstract String getUserInfo();


    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(RestService.class, RestServiceImpl.class);
    }

    @Override
    protected Map<String,String> startupConfigProperties() {
        Map<String,String> config = new HashMap<>(super.startupConfigProperties());
        config.put("fdbsql.http.cross_origin.enabled", "true");
        config.put("fdbsql.http.cross_origin.allowed_methods", ALLOWED_METHODS);
        config.put("fdbsql.http.cross_origin.allowed_origins", "*");
        return config;
    }

    private String entityEndpoint() {
        return String.format("%s/entity/%s.%s", restContext, SCHEMA, TABLE);
    }

    private static String headerValue(HttpResponse response, String key) {
        Header header = response.getFirstHeader(key);
        return (header != null) ? header.getValue() : null;
    }

    @Test
    public void requestBlockedWithMissingReferer() throws Exception{
        URI uri = new URI("http", null /*preflight requires no auth*/, "localhost", port, entityEndpoint(), null, null);

        HttpUriRequest request = new HttpGet(uri);

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_BAD_REQUEST, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("server.properties"));
    }
}
