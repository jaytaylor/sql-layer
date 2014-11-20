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

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.rest.RestService;
import com.foundationdb.rest.RestServiceImpl;
import com.foundationdb.server.service.monitor.MonitorService;
import com.foundationdb.server.service.plugins.PluginITBase;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.security.SecurityServiceImpl;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.sql.embedded.EmbeddedJDBCService;
import com.foundationdb.sql.embedded.EmbeddedJDBCServiceImpl;

public class HttpMonitorVerifyIT extends PluginITBase {
    
    private static final Logger LOG = LoggerFactory.getLogger(HttpMonitorVerifyIT.class);

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
            .bindAndRequire(SecurityService.class, SecurityServiceImpl.class)
            .bindAndRequire(EmbeddedJDBCService.class, EmbeddedJDBCServiceImpl.class)
            .bindAndRequire(RestService.class, RestServiceImpl.class);
    }

    @Before
    public void setUp() {
        SecurityService securityService = securityService();
        securityService.addRole("rest-user");
        securityService.addUser("user1", "password", Arrays.asList("rest-user"));
    }

    protected SecurityService securityService() {
        return serviceManager().getServiceByClass(SecurityService.class);
    }
    
    protected HttpConductor httpConductor() {
        return serviceManager().getServiceByClass(HttpConductor.class);
    }
    
    protected MonitorService monitorService () {
        return serviceManager().getServiceByClass(MonitorService.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("fdbsql.rest.login", "basic"); // "digest"
        properties.put("fdbsql.restrict_user_schema", "true");
        return properties;
    }

    private static int openRestURL(HttpClient client, String userInfo, int port, String path) throws Exception {
        URI uri = new URI("http", userInfo, "localhost", port, path, null, null);
        HttpGet get = new HttpGet(uri);
        HttpResponse response = client.execute(get);
        int code = response.getStatusLine().getStatusCode();
        EntityUtils.consume(response.getEntity());
        return code;
    }

    @Test
    public void runTest () throws Exception {
        MonitorService monitor = monitorService();
        
        CloseableHttpClient client = HttpClientBuilder.create().build();
        openRestURL(client, "user1:password", httpConductor().getPort(), "/version");
        assertEquals(monitor.getSessionMonitors().size(), 1);
        
        client.close();
    }

}
