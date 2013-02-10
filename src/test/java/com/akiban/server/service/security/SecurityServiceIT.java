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
import com.akiban.sql.embedded.EmbeddedJDBCService;
import com.akiban.sql.embedded.EmbeddedJDBCServiceImpl;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SecurityServiceIT extends ITBase
{
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
            .bindAndRequire(SecurityService.class, SecurityServiceImpl.class)
            .bindAndRequire(EmbeddedJDBCService.class, EmbeddedJDBCServiceImpl.class)
            .bindAndRequire(RestService.class, RestServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("akserver.http.login", "basic"); // "digest"
        properties.put("akserver.postgres.login", "md5");
        properties.put("akserver.restrict_user_schema", "true");
        return properties;
    }

    protected SecurityService securityService() {
        return serviceManager().getServiceByClass(SecurityService.class);
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
    }

    @Test
    public void authenticate() {
        assertEquals("user1", securityService().authenticate(session(), "user1", "password").getName());
    }

    private int openRestURL(String request, String query, String userInfo)
            throws Exception {
        int port = serviceManager().getServiceByClass(com.akiban.http.HttpConductor.class).getPort();
        String context = serviceManager().getServiceByClass(com.akiban.rest.RestService.class).getContextPath();
        URI uri = new URI("http", userInfo, "localhost", port, context + request, query, null);
        HttpGet get = new HttpGet(uri);
        HttpClient client = new DefaultHttpClient();
        HttpResponse response = client.execute(get);
        System.out.println("*** " + response);
        int code = response.getStatusLine().getStatusCode();
        EntityUtils.consume(response.getEntity());
        client.getConnectionManager().shutdown();
        return code;
    }

    @Test
    public void restUnauthenticated() throws Exception {
        assertEquals(HttpStatus.SC_UNAUTHORIZED,
                     openRestURL("/user1.utable/1", null, null));
    }

    @Test
    public void restAuthenticated() throws Exception {
        assertEquals(HttpStatus.SC_OK,
                     openRestURL("/user1.utable/1", null, "user1:password"));
    }

    @Test
    public void restAuthenticateBadUser() throws Exception {
        assertEquals(HttpStatus.SC_UNAUTHORIZED,
                     openRestURL("/user1.utable/1", null, "user2:none"));
    }

    @Test
    public void restAuthenticateBadPassword() throws Exception {
        assertEquals(HttpStatus.SC_UNAUTHORIZED,
                     openRestURL("/user1.utable/1", null, "user1:wrong"));
    }

    @Test
    public void restAuthenticateWrongSchema() throws Exception {
        assertEquals(HttpStatus.SC_FORBIDDEN,
                     openRestURL("/user2.utable/1", null, "user1:password"));
    }

    @Test
    public void restQueryAuthenticated() throws Exception {
        assertEquals(HttpStatus.SC_OK,
                     openRestURL("/query", "q=SELECT+*+FROM+utable", "user1:password"));
    }

    @Test
    public void restQueryWrongSchema() throws Exception {
        assertEquals(HttpStatus.SC_NOT_FOUND,
                     openRestURL("/query", "q=SELECT+*+FROM+user2.utable", "user1:password"));
    }

    private Connection openPostgresConnection(String user, String password) 
            throws Exception {
        int port = serviceManager().getServiceByClass(com.akiban.sql.pg.PostgresService.class).getPort();
        Class.forName("org.postgresql.Driver");
        String url = String.format("jdbc:postgresql://localhost:%d/%s", port, user);
        return DriverManager.getConnection(url, user, password);
    }

    @Test(expected = SQLException.class)
    public void postgresUnauthenticated() throws Exception {
        openPostgresConnection(null, null).close();
    }

    @Test
    public void postgresAuthenticated() throws Exception {
        Connection conn = openPostgresConnection("user1", "password");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT id FROM utable");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        rs.close();
        stmt.execute("DROP TABLE utable");
        stmt.close();
        conn.close();
    }

    @Test(expected = SQLException.class)
    public void postgresBadUser() throws Exception {
        openPostgresConnection("user2", "whatever").close();
    }

    @Test(expected = SQLException.class)
    public void postgresBadPassword() throws Exception {
        openPostgresConnection("user1", "nope").close();
    }

    @Test(expected = SQLException.class)
    public void postgresWrongSchema() throws Exception {
        Connection conn = openPostgresConnection("user1", "password");
        Statement stmt = conn.createStatement();
        stmt.executeQuery("SELECT id FROM user2.utable");
    }

    @Test(expected = SQLException.class)
    public void postgresWrongSchemaDDL() throws Exception {
        Connection conn = openPostgresConnection("user1", "password");
        Statement stmt = conn.createStatement();
        stmt.executeQuery("DROP TABLE user2.utable");
    }

}
