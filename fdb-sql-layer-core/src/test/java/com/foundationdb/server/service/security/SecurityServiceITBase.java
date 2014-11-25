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

import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.sql.embedded.EmbeddedJDBCService;
import com.foundationdb.sql.embedded.EmbeddedJDBCServiceImpl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public abstract class SecurityServiceITBase extends ITBase
{
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
            .bindAndRequire(EmbeddedJDBCService.class, EmbeddedJDBCServiceImpl.class)
            .bindAndRequire(SecurityService.class, SecurityServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("fdbsql.restrict_user_schema", "true");
        return properties;
    }

    @Before
    public void setUp() {
        int t1 = createTable("user1", "utable", "id int primary key not null");
        int t2 = createTable("user2", "utable", "id int primary key not null");        
        writeRow(t1, 1L);
        writeRow(t2, 2L);
        
        SecurityService securityService = securityService();
        securityService.addRole("rest-user");
        securityService.addRole("admin");
        securityService.addUser("user1", "password", Arrays.asList("rest-user"));
        securityService.addUser("akiban", "topsecret", Arrays.asList("rest-user", "admin"));
    }

    @After
    public void cleanUp() {
        securityService().clearAll(session());
    }

    @Test
    public void getUser() {
        SecurityService securityService = securityService();
        User user = securityService.getUser("user1");
        assertNotNull("user found", user);
        assertTrue("user has role", user.hasRole("rest-user"));
        assertFalse("user does not have role", user.hasRole("admin"));
        assertEquals("users roles", "[rest-user]", user.getRoles().toString());
        assertEquals("user password basic", "MD5:5F4DCC3B5AA765D61D8327DEB882CF99", user.getBasicPassword());
        assertEquals("user password digest", "MD5:BDAA29D9E7DCE23995599F595AA8832D", user.getDigestPassword());
    }

    @Test
    public void authenticate() {
        assertEquals("user1", securityService().authenticate(session(), "user1", "password").getName());
    }

}
