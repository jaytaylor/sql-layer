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
import com.foundationdb.server.service.plugins.PluginITBase;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public abstract class RestServiceITBase extends PluginITBase {
    protected static final String SCHEMA = "test";
    protected static final String TABLE = "t";
    protected int port;
    protected String restContext;
    protected HttpResponse response;
    protected CloseableHttpClient client;

    protected abstract String getUserInfo();

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


    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(RestService.class, RestServiceImpl.class);
    }

    protected static String headerValue(HttpResponse response, String key) {
        Header header = response.getFirstHeader(key);
        return (header != null) ? header.getValue() : null;
    }

    protected String entityEndpoint() {
        return String.format("%s/entity/%s.%s", restContext, SCHEMA, TABLE);
    }

    protected URI defaultURI() throws URISyntaxException {
        return defaultURI("");
    }

    protected URI defaultURI(String entitySuffix) throws URISyntaxException {
        return new URI("http", getUserInfo(), "localhost", port, entityEndpoint() + entitySuffix, null, null);
    }
}
