/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.service.security;

import com.akiban.rest.RestService;
import com.akiban.rest.RestServiceImpl;
import com.akiban.server.service.servicemanager.GuicedServiceManager;
import com.akiban.server.test.it.ITBase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SecurityServiceIT extends ITBase
{
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
            .bindAndRequire(SecurityService.class, SecurityServiceImpl.class)
            .bindAndRequire(RestService.class, RestServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("akserver.http.login", "true");
        properties.put("akserver.postgres.login", "md5");
        return properties;
    }

    protected SecurityService securityService() {
        return serviceManager().getServiceByClass(SecurityService.class);
    }

    @Before
    public void createUsers() {
        SecurityService securityService = securityService();
        securityService.addRole("rest-user");
        securityService.addRole("admin");
        securityService.addUser("user1", "password", Arrays.asList("rest-user"));
    }

    @After
    public void cleanUp() {
        securityService().clearAll();
    }

    @Test
    public void getUser() {
        SecurityService securityService = securityService();
        User user = securityService.getUser("user1");
        assertNotNull("user found", user);
        assertTrue("user has role", user.hasRole("rest-user"));
        assertFalse("user does not have role", user.hasRole("admin"));
    }

    @Test
    public void authenticate() {
        assertEquals("user1", securityService().authenticate("user1", "password").getName());
    }

    private URL getRestURL(String userInfo, String request) 
            throws MalformedURLException, URISyntaxException {
        int port = serviceManager().getServiceByClass(com.akiban.http.HttpConductor.class).getPort();
        String context = serviceManager().getServiceByClass(com.akiban.rest.RestService.class).getContextPath();
        return new URI("http", userInfo, "localhost", port, context + request, null, null).toURL();
    }

    @Test(expected = IOException.class)
    public void restUnauthenticated() throws Exception {
        URL url = getRestURL(null, "/version");
        Object c = url.openConnection().getContent();
    }

    @Test
    public void restAuthenticated() throws Exception {
        URL url = getRestURL("user1:password", "/version");
        Object c = url.openConnection().getContent();
        System.out.println("*** " + c);
    }

}
