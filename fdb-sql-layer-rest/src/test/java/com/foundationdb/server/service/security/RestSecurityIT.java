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

package com.foundationdb.server.service.security;

import com.foundationdb.http.HttpConductor;
import com.foundationdb.rest.RestService;
import com.foundationdb.rest.RestServiceImpl;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.ITBase;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.foundationdb.util.JsonUtils.readTree;
import static org.junit.Assert.*;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RestSecurityIT extends SecurityServiceITBase
{
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
            .bindAndRequire(RestService.class, RestServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String, String> properties = super.startupConfigProperties();
        properties.put("fdbsql.http.login", "basic"); // "digest"
        properties.put("fdbsql.http.csrf_protection.type", "none");
        return properties;
    }

    private int openRestURL(String request, String query, String userInfo, boolean post)
            throws Exception {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpRequestBase httpRequest;
        if (post) {
            httpRequest = new HttpPost(getRestURL(request, "", userInfo));
            ((HttpPost)httpRequest).setEntity(new ByteArrayEntity(query.getBytes("UTF-8")));
        } else {
            httpRequest = new HttpGet(getRestURL(request, query, userInfo));
        }
        HttpResponse response = client.execute(httpRequest);
        int code = response.getStatusLine().getStatusCode();
        EntityUtils.consume(response.getEntity());
        client.close();
        return code;
    }

    private URI getRestURL(String request, String query, String userInfo)
            throws Exception {
        int port = serviceManager().getServiceByClass(HttpConductor.class).getPort();
        String context = serviceManager().getServiceByClass(RestService.class).getContextPath();
        return new URI("http", userInfo, "localhost", port, context + request, query, null);
    }

    @Test
    public void restUnauthenticated() throws Exception {
        assertEquals(HttpStatus.SC_UNAUTHORIZED,
                     openRestURL("/user1.utable/1", null, null, false));
    }

    @Test
    public void restAuthenticated() throws Exception {
        assertEquals(HttpStatus.SC_OK,
                     openRestURL("/entity/user1.utable/1", null, "user1:password", false));
    }

    @Test
    public void restAuthenticateBadUser() throws Exception {
        assertEquals(HttpStatus.SC_UNAUTHORIZED,
                     openRestURL("/user1.utable/1", null, "user2:none", false));
    }

    @Test
    public void restAuthenticateBadPassword() throws Exception {
        assertEquals(HttpStatus.SC_UNAUTHORIZED,
                     openRestURL("/user1.utable/1", null, "user1:wrong", false));
    }

    @Test
    public void restAuthenticateWrongSchema() throws Exception {
        assertEquals(HttpStatus.SC_FORBIDDEN,
                     openRestURL("/entity/user2.utable/1", null, "user1:password", false));
    }

    @Test
    public void restQueryAuthenticated() throws Exception {
        assertEquals(HttpStatus.SC_OK,
                     openRestURL("/sql/query", "{\"q\": \"SELECT * FROM utable\"}", "user1:password", true));
    }

    @Test
    public void restQueryWrongSchema() throws Exception {
        assertEquals(HttpStatus.SC_NOT_FOUND,
                     openRestURL("/sql/query", "{\"q\": \"SELECT * FROM user2.utable\"}", "user1:password", true));
    }

    static final String ADD_USER = "{\"user\":\"user3\", \"password\":\"pass\", \"roles\": [\"rest-user\"]}";

    @Test
    public void restAddDropUser() throws Exception {
        SecurityService securityService = securityService();
        assertNull(securityService.getUser("user3"));
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(getRestURL("/security/users", null, "akiban:topsecret"));
        post.setEntity(new StringEntity(ADD_USER, ContentType.APPLICATION_JSON));
        HttpResponse response = client.execute(post);
        int code = response.getStatusLine().getStatusCode();
        String content = EntityUtils.toString(response.getEntity());
        assertEquals(HttpStatus.SC_OK, code);
        assertNotNull(securityService.getUser("user3"));

        // Check returned id
        JsonNode idNode = readTree(content).get("id");
        assertNotNull("Has id field", idNode);
        assertEquals("id is integer", true, idNode.isInt());

        HttpDelete delete = new HttpDelete(getRestURL("/security/users/user3", null, "akiban:topsecret"));
        response = client.execute(delete);
        code = response.getStatusLine().getStatusCode();
        EntityUtils.consume(response.getEntity());
        client.close();
        assertEquals(HttpStatus.SC_OK, code);
        assertNull(securityService.getUser("user3"));
    }

}
