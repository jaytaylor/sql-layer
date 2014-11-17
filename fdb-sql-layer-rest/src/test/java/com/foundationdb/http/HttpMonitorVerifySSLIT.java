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
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.rest.RestService;
import com.foundationdb.rest.RestServiceImpl;
import com.foundationdb.server.service.monitor.MonitorService;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.security.SecurityServiceImpl;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.sql.embedded.EmbeddedJDBCService;
import com.foundationdb.sql.embedded.EmbeddedJDBCServiceImpl;

/**
 * In order to run this test, you need to generate a key store, then update the
 * javax.net.ssl.keyStore system property (set below), to the path where you put the 
 * keystore file.
 * <pre> 
 *  $ keytool -keystore keystore -alias akiban -genkey -keyalg RSA 
 *  Enter keystore password: password  
 *  Re-enter new password: password 
 *  What is your first and last name?
 *    [Unknown]:  akiban.com
 *  What is the name of your organizational unit?
 *    [Unknown]:  akiban.com
 *  What is the name of your organization?
 *    [Unknown]:  akiban
 *  What is the name of your City or Locality?
 *    [Unknown]:  
 *  What is the name of your State or Province?
 *    [Unknown]:  
 *  What is the two-letter country code for this unit?
 *    [Unknown]:  
 *  Is CN=akiban.com, OU=akiban.com, O=akiban, L=Unknown, ST=Unknown, C=Unknown correct?
 *    [no]:  yes
 * </pre>   
 * Because we don't want to be shipping a half completed SSL certificate with the source 
 * code, This is a manual step required for this (otherwise) disabled test. 
 * @author tjoneslo
 *
 */
public class HttpMonitorVerifySSLIT extends ITBase {
    private static final Logger LOG = LoggerFactory.getLogger(HttpMonitorVerifySSLIT.class);

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
        securityService.addRole("admin");
        securityService.addUser("user1", "password", Arrays.asList("rest-user"));
        securityService.addUser("akiban", "topsecret", Arrays.asList("rest-user", "admin"));
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
        properties.put("plugins.rest.login", "digest"); // "digest"
        properties.put("plugins.rest.ssl", "true");
        properties.put("fdbsql.restrict_user_schema", "true");
        
        Properties p = new Properties(System.getProperties());
        p.put("javax.net.ssl.keyStore", "./keystore");
        p.put("javax.net.ssl.keyStorePassword", "password");
        System.setProperties(p);
        return properties;
    }

    private static int openRestURL(HttpClient client, String userInfo, int port, String path) throws Exception {
        URI uri = new URI("https", userInfo, "localhost", port, path, null, null);
        HttpGet get = new HttpGet(uri);
        HttpResponse response = client.execute(get);
        int code = response.getStatusLine().getStatusCode();
        EntityUtils.consume(response.getEntity());
        return code;
    }

    @Ignore ("need setup")
    @Test
    public void runTest () throws Exception {
        MonitorService monitor = monitorService();
        
        CloseableHttpClient client = createClient();
        
        openRestURL(client, "user1:password", httpConductor().getPort(), "/version");
        
        assertEquals(monitor.getSessionMonitors().size(), 1);
        
        client.close();
    }

 
    /**
     * This code sets up the httpclient to accept any SSL certificate. The 
     * SSL certificate generated by the instructions above is not correctly
     * signed, so we need ignore the problem. 
     * This code should not, under any circumstances, be allowed anywhere 
     * the production code. 
     * @return
     */
    private CloseableHttpClient createClient () {
        try {
            HttpClientBuilder builder = HttpClientBuilder.create();
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, new TrustManager[]{getTrustManager()}, null);
            SSLConnectionSocketFactory scsf = new SSLConnectionSocketFactory(ctx, SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
            builder.setSSLSocketFactory(scsf);
            Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("https", scsf)
                    .build();

            HttpClientConnectionManager ccm = new BasicHttpClientConnectionManager(registry);

            builder.setConnectionManager(ccm);
            return builder.build();
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    private X509TrustManager getTrustManager() {
        return new X509TrustManager() {
        
        public X509Certificate[] getAcceptedIssuers() {
        return null;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] arg0, String arg1)
                throws CertificateException { }

        @Override
        public void checkServerTrusted(X509Certificate[] arg0, String arg1)
                throws CertificateException {  }
        };
    }
}
