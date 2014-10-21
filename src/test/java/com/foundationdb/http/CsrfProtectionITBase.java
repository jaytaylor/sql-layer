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
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
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

public abstract class CsrfProtectionITBase extends ITBase
{
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";


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
        config.put("fdbsql.http.csrf_protection.allowed_referers", "http://somewhere.com,https://coolest.site.edu:4320");
        return config;
    }

    private String entityEndpoint() {
        return String.format("%s/entity/%s.%s", restContext, SCHEMA, TABLE);
    }

    @Test
    public void requestBlockedWithMissingReferer() throws Exception{
        URI uri = new URI("http", getUserInfo(), "localhost", port, entityEndpoint(), null, null);

        HttpUriRequest request = new HttpGet(uri);

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_BAD_REQUEST, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("CSRF attack prevented."));
    }

    @Test
    public void requestBlockedWithEmptyReferer() throws Exception{
        URI uri = new URI("http", getUserInfo(), "localhost", port, entityEndpoint(), null, null);

        HttpUriRequest request = new HttpGet(uri);
        request.setHeader("Referer","");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_BAD_REQUEST, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("CSRF attack prevented."));
    }

    @Test
    public void requestBlockedWithBadHost() throws Exception{
        URI uri = new URI("http", getUserInfo(), "localhost", port, entityEndpoint(), null, null);

        HttpUriRequest request = new HttpGet(uri);
        request.setHeader("Referer", "https://coolest.site.edu.fake.com:4320");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_BAD_REQUEST, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("CSRF attack prevented."));
    }

    @Test
    public void postBlockedWithBadHost() throws Exception{
        URI uri = new URI("http", getUserInfo(), "localhost", port, entityEndpoint(), null, null);

        HttpUriRequest request = new HttpPost(uri);
        request.setHeader("Referer", "https://coolest.site.edu.fake.com:4320");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_BAD_REQUEST, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("CSRF attack prevented."));
    }

    @Test
    public void putBlockedWithMissingReferer() throws Exception{
        URI uri = new URI("http", getUserInfo(), "localhost", port, entityEndpoint(), null, null);

        HttpUriRequest request = new HttpPut(uri);

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_BAD_REQUEST, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("CSRF attack prevented."));
    }


    @Test
    public void getAllowed1() throws Exception{
        URI uri = new URI("http", getUserInfo(), "localhost", port, entityEndpoint(), null, null);

        HttpUriRequest request = new HttpGet(uri);
        request.setHeader("Referer","http://somewhere.com");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    }

    @Test
    public void getAllowed2() throws Exception{
        URI uri = new URI("http", getUserInfo(), "localhost", port, entityEndpoint(), null, null);

        HttpUriRequest request = new HttpGet(uri);
        request.setHeader("Referer","https://coolest.site.edu:4320");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    }

    @Test
    public void postAllowed() throws Exception{
        URI uri = new URI("http", getUserInfo(), "localhost", port, entityEndpoint(), null, null);

        HttpPost request = new HttpPost(uri);
        request.setHeader("Referer","http://somewhere.com");
        request.setHeader("Content-Type", "application/json");
        request.setEntity(new StringEntity("{\"id\": \"1\"}"));

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    }

    @Test
    public void putAllowed() throws Exception{
        URI uri = new URI("http", getUserInfo(), "localhost", port, entityEndpoint() + "/1", null, null);

        HttpPut request = new HttpPut(uri);
        request.setHeader("Referer","http://somewhere.com");
        request.setHeader("Content-Type", "application/json");
        request.setEntity(new StringEntity("{\"id\": \"1\"}"));

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    }

    @Test
    public void deleteAllowed() throws Exception{
        URI uri = new URI("http", getUserInfo(), "localhost", port, entityEndpoint() + "/1", null, null);

        HttpUriRequest request = new HttpDelete(uri);
        request.setHeader("Referer","http://somewhere.com");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_NO_CONTENT, response.getStatusLine().getStatusCode());
    }
}
