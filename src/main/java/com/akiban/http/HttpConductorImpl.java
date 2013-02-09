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

package com.akiban.http;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.security.SecurityService;
import com.google.inject.Inject;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.JDBCLoginService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.security.authentication.DigestAuthenticator;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.LogManager;

public final class HttpConductorImpl implements HttpConductor, Service {
    private static final Logger logger = LoggerFactory.getLogger(HttpConductorImpl.class);
    private static final String PORT_PROPERTY = "akserver.http.port";
    private static final String SSL_PROPERTY = "akserver.http.ssl";
    private static final String LOGIN_PROPERTY = "akserver.http.login";

    private static final String REST_ROLE = "rest-user";

    private final ConfigurationService configurationService;
    private final SecurityService securityService;

    private final Object lock = new Object();
    private HandlerCollection handlerCollection;
    private Server server;
    private Set<String> registeredPaths;
    private volatile int port = -1;

    // Need reference to prevent GC and setting loss
    private final java.util.logging.Logger jerseyLogging;

    @Inject
    public HttpConductorImpl(ConfigurationService configurationService,
                             SecurityService securityService) {
        this.configurationService = configurationService;
        this.securityService = securityService;

        jerseyLogging = java.util.logging.Logger.getLogger("com.sun.jersey");
        jerseyLogging.setLevel(java.util.logging.Level.OFF);
    }

    @Override
    public void registerHandler(ContextHandler handler) {
        String contextBase = getContextPathPrefix(handler.getContextPath());
        synchronized (lock) {
            if (registeredPaths == null)
                registeredPaths = new HashSet<>();
            if (!registeredPaths.add(contextBase))
                throw new IllegalPathRequest("context already reserved: " + contextBase);
            handlerCollection.addHandler(handler);
            if (!handler.isStarted()) {
                try {
                    handler.start();
                }
                catch (Exception e) {
                    throw new HttpConductorException(e);
                }
            }
        }
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public void unregisterHandler(ContextHandler handler) {
        String contextBase = getContextPathPrefix(handler.getContextPath());
        synchronized (lock) {
            if (registeredPaths == null || (!registeredPaths.remove(contextBase))) {
                logger.warn("Path not registered (for " + handler + "): " + contextBase);
            }
            else {
                handlerCollection.removeHandler(handler);
                if (!handler.isStopped()) {
                    // As of the current version of jetty, HandlerCollection#removeHandler stops the handler -- so
                    // this block won't get executed. This is really here for future-proofing, in case that auto-stop
                    // goes away for some reason.
                    try {
                        handler.stop();
                    }
                    catch (Exception e) {
                        throw new HttpConductorException(e);
                    }
                }
            }
        }
    }

    static enum AuthenticationType {
        NONE, BASIC, DIGEST
    }

    @Override
    public void start() {
        String portProperty = configurationService.getProperty(PORT_PROPERTY);
        String sslProperty = configurationService.getProperty(SSL_PROPERTY);
        String loginProperty = configurationService.getProperty(LOGIN_PROPERTY);
        int portLocal;
        boolean ssl;
        AuthenticationType login;
        try {
            portLocal = Integer.parseInt(portProperty);
        }
        catch (NumberFormatException e) {
            logger.error("bad port descriptor: " + portProperty);
            throw e;
        }
        ssl = Boolean.parseBoolean(sslProperty);
        if ("none".equals(loginProperty)) {
            login = AuthenticationType.NONE;
        }
        else if ("basic".equals(loginProperty)) {
            login = AuthenticationType.BASIC;
        }
        else if ("digest".equals(loginProperty)) {
            login = AuthenticationType.DIGEST;
        }
        else {
            throw new IllegalArgumentException("Invalid " + LOGIN_PROPERTY +
                                               " property: " + loginProperty);
        }
        logger.info("Starting {} service on port {} with authentication {}", 
                    new Object[] { ssl ? "HTTPS" : "HTTP", portProperty, login });
                    
        Server localServer = new Server();
        SelectChannelConnector connector;
        if (!ssl) {
            connector = new SelectChannelConnector();
        }
        else {
            // Share keystore configuration with PSQL.
            SslContextFactory sslFactory = new SslContextFactory();
            sslFactory.setKeyStorePath(System.getProperty("javax.net.ssl.keyStore"));
            sslFactory.setKeyStorePassword(System.getProperty("javax.net.ssl.keyStorePassword"));
            connector = new SslSelectChannelConnector(sslFactory);
        }
        connector.setPort(portLocal);
        connector.setThreadPool(new QueuedThreadPool(200));
        connector.setAcceptors(4);
        connector.setMaxIdleTime(300000);
        connector.setAcceptQueueSize(12000);
        connector.setLowResourcesConnections(25000);
        localServer.setConnectors(new Connector[]{connector});

        HandlerCollection localHandlerCollection = new HandlerCollection(true);

        try {
            if (login == AuthenticationType.NONE) {
                localServer.setHandler(localHandlerCollection);
            }
            else {
                String resource;
                Authenticator authenticator;
                switch (login) {
                case BASIC:
                    resource = "basic.properties";
                    authenticator = new BasicAuthenticator();
                    break;
                case DIGEST:
                    resource = "digest.properties";
                    authenticator = new DigestAuthenticator();
                    break;
                default:
                    assert false : "Unexpected authentication type " + login;
                    resource = null;
                    authenticator = null;
                }
                Constraint constraint = new Constraint(authenticator.getAuthMethod(),
                                                       REST_ROLE);
                constraint.setAuthenticate(true);

                ConstraintMapping cm = new ConstraintMapping();
                cm.setPathSpec("/*");
                cm.setConstraint(constraint);

                ConstraintSecurityHandler sh = new ConstraintSecurityHandler();
                sh.setAuthenticator(authenticator);
                sh.setConstraintMappings(Collections.singletonList(cm));

                LoginService loginService =
                    new SingleThreadJDBCLoginService(SecurityService.REALM, 
                                                     HttpConductorImpl.class.getResource(resource).toString());
                sh.setLoginService(loginService);

                sh.setHandler(localHandlerCollection);
                localServer.setHandler(sh);
            }
            localServer.start();
        }
        catch (Exception e) {
            logger.error("failed to start HTTP server", e);
            throw new HttpConductorException(e);
        }

        synchronized (lock) {
            this.server = localServer;
            this.handlerCollection = localHandlerCollection;
            this.registeredPaths = null;
            this.port = portLocal;
        }
    }

    @Override
    public void stop() {
        Server localServer;
        synchronized (lock) {
            localServer = server;
            server = null;
            handlerCollection = null;
            registeredPaths = null;
            port = -1;
        }
        try {
            localServer.stop();
        }
        catch (Exception e) {
            logger.error("failed to stop HTTP server", e);
            throw new HttpConductorException(e);
        }
    }

    @Override
    public void crash() {
        stop();
    }

    static String getContextPathPrefix(String contextPath) {
        if (!contextPath.startsWith("/"))
            throw new IllegalPathRequest("registered paths must start with '/'");
        int contextBaseEnd = contextPath.indexOf("/", 1);
        if (contextBaseEnd < 0)
            contextBaseEnd = contextPath.length();
        String result = contextPath.substring(1, contextBaseEnd);
        if (result.contains("*"))
            throw new IllegalPathRequest("can't ask for a glob within the first URL segment");
        return result;
    }

    // Embedded JDBC is single-threaded, but login service assumes it
    // is thread-safe.  Also, it does not close its ResultSet, which
    // leaves a transaction active. So just close connection around
    // each use.  Login service is already
    // synchronized. Unfortunately, this needs a private method.
    static class SingleThreadJDBCLoginService extends JDBCLoginService {
        private Method closeMethod;

        public SingleThreadJDBCLoginService(String name, String config)
                throws IOException {
            super(name, config);
            try {
                closeMethod = JDBCLoginService.class.getDeclaredMethod("closeConnection", null);
                closeMethod.setAccessible(true);
            }
            catch (Exception ex) {
                throw new AkibanInternalException("Cannot get JDBC close method", ex);
            }
        }

        @Override
        protected org.eclipse.jetty.server.UserIdentity loadUser(String username) {
            try {
                return super.loadUser(username);
            }
            finally {
                try {
                    closeMethod.invoke(this);
                }
                catch (Exception ex) {
                    logger.warn("Cannot call JDBC close method", ex);
                }
            }
        }
    }
}
