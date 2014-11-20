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

import com.foundationdb.junit.SelectedParameterizedRunner;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.security.SecurityServiceImpl;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.containsString;

@RunWith(SelectedParameterizedRunner.class)
public class AuthRealmIT extends RestServiceITBase {
    private static final String LOGIN_PROPERTY = "fdbsql.rest.login";
    private static final String REALM_PROPERTY = "fdbsql.security.realm";

    private static final String ROLE = "rest-user";
    private static final String USER = "u";
    private static final String PASS = "p";

    private final String authType;
    private final String realm;
    private String expectedRealm;


    @Parameterized.Parameters(name="{0} auth with realm={1}")
    public static Iterable<Object[]> queries() throws Exception {
        // null in list below means use system defaults
        return Arrays.asList(
                new Object[] {"basic", null},
                new Object[] {"basic", ""},
                new Object[] {"basic", "My realm"},
                new Object[] {"digest", null},
                new Object[] {"digest", ""},
                new Object[] {"digest", "My realm"});
    }

    public AuthRealmIT(String authType, String realm) {
        this.authType = authType;
        this.realm = realm;
    }

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(SecurityService.class, SecurityServiceImpl.class);
    }

    @Override
    protected Map<String,String> startupConfigProperties() {
        Map<String,String> config = new HashMap<>(super.startupConfigProperties());
        if (authType != null) {
            config.put(LOGIN_PROPERTY, authType);
        }
        if (realm != null) {
            config.put(REALM_PROPERTY, realm);
        }

        return config;
    }

    @Override
    protected String getUserInfo() {
        return USER + ":" + PASS;
    }

    @Before
    public final void createUser() {
        if(realm == null) {
            expectedRealm = configService().getProperty(REALM_PROPERTY);
        } else {
            expectedRealm = realm;
        }
        SecurityService securityService = securityService();
        securityService.addRole(ROLE);
        securityService.addUser(USER, PASS, Arrays.asList(ROLE));
    }

    @After
    public final void clearUser() {
        securityService().clearAll(session());
    }

    @Test
    public void testRealmIsSetInHeader() throws Exception{
        URI uri = new URI("http", null, "localhost", port, entityEndpoint() + "", null, null);
        HttpUriRequest request = new HttpGet(uri);

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_UNAUTHORIZED, response.getStatusLine().getStatusCode());
            assertThat("reason", headerValue(response, "WWW-Authenticate"),
                    containsString("realm=\"" + expectedRealm + "\""));
    }

    @Test
    public void testGet() throws Exception {
        HttpUriRequest request = new HttpGet(defaultURI());
        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
        assertThat("response", EntityUtils.toString(response.getEntity()), is(not("")));
    }
}
