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

public abstract class CsrfProtectionITBase extends RestServiceITBase
{

    protected abstract String getUserInfo();

    @Override
    protected Map<String,String> startupConfigProperties() {
        Map<String,String> config = new HashMap<>(super.startupConfigProperties());
        config.put("plugins.rest.csrf_protection.allowed_referers", "http://somewhere.com,https://coolest.site.edu:4320");
        return config;
    }

    @Test
    public void requestBlockedWithMissingReferer() throws Exception{
        HttpUriRequest request = new HttpPut(defaultURI());

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_FORBIDDEN, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("Referer"));
    }

    @Test
    public void requestBlockedWithEmptyReferer() throws Exception{
        HttpUriRequest request = new HttpPut(defaultURI());
        request.setHeader("Referer","");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_FORBIDDEN, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("Referer"));
    }

    @Test
    public void getBlockedWithBadHost() throws Exception{
        // Although we let blank & empty referers through for get requests, there is no benefit to
        // letting incorrect referers through, so those are always blocked.
        HttpUriRequest request = new HttpGet(defaultURI());
        request.setHeader("Referer", "https://coolest.site.edu.fake.com:4320");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_FORBIDDEN, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("Referer"));
    }

    @Test
    public void postBlockedWithBadHost() throws Exception{
        HttpUriRequest request = new HttpPost(defaultURI());
        request.setHeader("Referer", "https://coolest.site.edu.fake.com:4320");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_FORBIDDEN, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("Referer"));
    }

    @Test
    public void putBlockedWithMissingReferer() throws Exception{
        HttpUriRequest request = new HttpPut(defaultURI());

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_FORBIDDEN, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("Referer"));
    }
    @Test
    public void putBlockedWithBlankReferer() throws Exception{
        HttpUriRequest request = new HttpPut(defaultURI());
        request.setHeader("Referer","");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_FORBIDDEN, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("Referer"));
    }

    @Test
    public void getAllowedWithNoReferer() throws Exception{
        // Since GET requests don't have side effects, the cross-origin header will prevent
        // third-party javascript from viewing the result, meaning that we can allow this through.
        HttpUriRequest request = new HttpGet(defaultURI());

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    }

    @Test
    public void getAllowedWithBlankReferer() throws Exception{
        // Since GET requests don't have side effects, the cross-origin header will prevent
        // third-party javascript from viewing the result, meaning that we can allow this through.
        HttpUriRequest request = new HttpGet(defaultURI());
        request.setHeader("Referer","");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    }

    @Test
    public void getAllowed1() throws Exception{
        HttpUriRequest request = new HttpGet(defaultURI());
        request.setHeader("Referer","http://somewhere.com");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    }

    @Test
    public void getAllowed2() throws Exception{
        HttpUriRequest request = new HttpGet(defaultURI());
        request.setHeader("Referer","https://coolest.site.edu:4320");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    }

    @Test
    public void postAllowed() throws Exception{
        HttpPost request = new HttpPost(defaultURI());
        request.setHeader("Referer","http://somewhere.com");
        request.setHeader("Content-Type", "application/json");
        request.setEntity(new StringEntity("{\"id\": \"1\"}"));

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    }

    @Test
    public void putAllowed() throws Exception{
        HttpPut request = new HttpPut(defaultURI("/1"));
        request.setHeader("Referer","http://somewhere.com");
        request.setHeader("Content-Type", "application/json");
        request.setEntity(new StringEntity("{\"id\": \"1\"}"));

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    }

    @Test
    public void deleteAllowed() throws Exception{
        HttpUriRequest request = new HttpDelete(defaultURI("/1"));
        request.setHeader("Referer","http://somewhere.com");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_NO_CONTENT, response.getStatusLine().getStatusCode());
    }
}
