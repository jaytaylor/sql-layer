/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpUriRequest;
import org.junit.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

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
public abstract class CrossOriginITBase extends RestServiceITBase {
    private static final String ORIGIN = "http://example.com";
    private static final String ALLOWED_METHODS = "GET,POST,PUT";
    private static final String HEADER_ALLOW_ORIGIN = "Access-Control-Allow-Origin";


    @Override
    protected Map<String,String> startupConfigProperties() {
        Map<String,String> config = new HashMap<>(super.startupConfigProperties());
        config.put("fdbsql.http.cross_origin.enabled", "true");
        config.put("fdbsql.http.cross_origin.allowed_methods", ALLOWED_METHODS);
        config.put("fdbsql.http.cross_origin.allowed_origins", "*");
        config.put("fdbsql.http.csrf_protection.type", "none");
        return config;
    }


    @Test
    public void preFlightAllowedMethod() throws Exception {
        URI uri = new URI("http", null /*preflight requires no auth*/, "localhost", port, entityEndpoint(), null, null);

        HttpUriRequest request = new HttpOptions(uri);
        request.setHeader("Origin", ORIGIN);
        request.setHeader("Access-Control-Request-Method", "PUT");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
        assertEquals("Allow-Origin", ORIGIN, headerValue(response, HEADER_ALLOW_ORIGIN));
    }

    @Test
    public void preFlightDisallowedMethod() throws Exception {
        URI uri = new URI("http", null /*preflight requires no auth*/, "localhost", port, entityEndpoint(), null, null);

        HttpUriRequest request = new HttpOptions(uri);
        request.setHeader("Origin", ORIGIN);
        request.setHeader("Access-Control-Request-Method", "DELETE");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
        assertEquals("Allow-Origin", null, headerValue(response, HEADER_ALLOW_ORIGIN));
    }

    @Test
    public void simpleMethod() throws Exception {
        HttpUriRequest request = new HttpGet(defaultURI());
        request.setHeader("Origin", ORIGIN);

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
        assertEquals("Allow-Origin", ORIGIN, headerValue(response, HEADER_ALLOW_ORIGIN));
    }

    @Test
    public void nonSimpleMethod() throws Exception {
        HttpUriRequest request = new HttpDelete(defaultURI("/1"));
        request.setHeader("Origin", ORIGIN);

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_NO_CONTENT, response.getStatusLine().getStatusCode());
        assertEquals("Allow-Origin", ORIGIN, headerValue(response, HEADER_ALLOW_ORIGIN));
    }
}
