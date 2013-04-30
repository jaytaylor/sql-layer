/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.http;

import com.akiban.server.service.security.SecurityService;
import com.akiban.server.service.security.SecurityServiceImpl;
import com.akiban.server.service.servicemanager.GuicedServiceManager;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CrossOriginWithAuthIT extends CrossOriginITBase
{
    private static final String ROLE = "rest-user";
    private static final String USER = "u";
    private static final String PASS = "p";


    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(SecurityService.class, SecurityServiceImpl.class);
    }

    @Override
    protected Map<String,String> startupConfigProperties() {
        Map<String,String> config = new HashMap<>(super.startupConfigProperties());
        config.put("akserver.http.login", "basic");
        return config;
    }

    @Override
    protected String getUserInfo() {
        return USER + ":" + PASS;
    }

    @Before
    public final void createUser() {
        SecurityService securityService = securityService();
        securityService.addRole(ROLE);
        securityService.addUser(USER, PASS, Arrays.asList(ROLE));
    }

    @After
    public final void clearUser() {
        securityService().clearAll(session());
    }
}
