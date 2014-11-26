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

import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.monitor.MonitorService;
import com.foundationdb.server.service.monitor.ServerMonitor;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.sql.server.ServerSessionMonitor;
import com.google.inject.Inject;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import org.eclipse.jetty.io.AsyncEndPoint;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.nio.AsyncConnection;
import org.eclipse.jetty.io.nio.SslConnection;
import org.eclipse.jetty.servlet.ServletMapping;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.security.authentication.DigestAuthenticator;
import org.eclipse.jetty.server.AsyncHttpConnection;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.FilterRegistration;
import javax.servlet.ServletException;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.foundationdb.http.SecurityServiceLoginService.CredentialType;

public final class HttpConductorImpl implements HttpConductor, Service {
    private static final Logger logger = LoggerFactory.getLogger(HttpConductorImpl.class);

    private static final String CONFIG_REALM = "fdbsql.security.realm"; // See also SecurityServiceImpl
    private static final String CONFIG_HTTP_PREFIX = "fdbsql.http.";
    private static final String CONFIG_HOST_PROPERTY = CONFIG_HTTP_PREFIX + "host";
    private static final String CONFIG_PORT_PROPERTY = CONFIG_HTTP_PREFIX + "port";
    private static final String CONFIG_SSL_PROPERTY = CONFIG_HTTP_PREFIX + "ssl";
    private static final String CONFIG_LOGIN_PROPERTY = CONFIG_HTTP_PREFIX + "login";
    private static final String CONFIG_LOGIN_CACHE_SECONDS = CONFIG_HTTP_PREFIX + "login_cache_seconds";
    private static final String CONFIG_XORIGIN_PREFIX = CONFIG_HTTP_PREFIX + "cross_origin.";
    private static final String CONFIG_XORIGIN_ENABLED = CONFIG_XORIGIN_PREFIX + "enabled";
    private static final String CONFIG_XORIGIN_ORIGINS = CONFIG_XORIGIN_PREFIX + "allowed_origins";
    private static final String CONFIG_XORIGIN_METHODS = CONFIG_XORIGIN_PREFIX + "allowed_methods";
    private static final String CONFIG_XORIGIN_HEADERS = CONFIG_XORIGIN_PREFIX + "allowed_headers";
    private static final String CONFIG_XORIGIN_MAX_AGE = CONFIG_XORIGIN_PREFIX + "preflight_max_age";
    private static final String CONFIG_XORIGIN_CREDENTIALS = CONFIG_XORIGIN_PREFIX + "allow_credentials";

    private static final String CONFIG_CSRF_PREFIX = CONFIG_HTTP_PREFIX + "csrf_protection.";
    private static final String CONFIG_CSRF_TYPE = CONFIG_CSRF_PREFIX + "type";
    private static final String CONFIG_CSRF_ALLOWED_REFERERS = CONFIG_CSRF_PREFIX + "allowed_referers";

    private static final String REST_ROLE = "rest-user";
    public  static final String SERVER_TYPE = "REST";

    private final ConfigurationService configurationService;
    private final SecurityService securityService;
    private final MonitorService monitorService;
    private final SessionService sessionService;

    private final Object lock = new Object();
    private ServletContextHandler rootContextHandler;
    private Server server;
    private Set<String> registeredPaths;
    private volatile int port = -1;

    // Need reference to prevent GC and setting loss
    private final java.util.logging.Logger jerseyLogging;

    @Inject
    public HttpConductorImpl(ConfigurationService configurationService,
                             SecurityService securityService,
                             MonitorService monitor,
                             SessionService session) {
        this.configurationService = configurationService;
        this.securityService = securityService;
        this.monitorService = monitor;
        this.sessionService = session;

        jerseyLogging = java.util.logging.Logger.getLogger("com.sun.jersey");
        jerseyLogging.setLevel(java.util.logging.Level.OFF);
    }

    @Override
    public void registerHandler(ServletHolder servlet, String path) {
        String contextBase = getContextPathPrefix(path);
        synchronized(lock) {
            if(!registeredPaths.add(contextBase)) {
                throw new IllegalPathRequest("context already reserved: " + contextBase);
            }
            try {
                rootContextHandler.addServlet(servlet, path);
                if(!servlet.isStarted()) {
                    servlet.start();
                }
            } catch (Exception e) {
                throw new HttpConductorException(e);
            }
        }
    }

    @Override
    public void unregisterHandler(ServletHolder servlet) {
        synchronized(lock) {
            ServletHandler servletHandler = rootContextHandler.getServletHandler();

            ServletHolder[] curServlets = servletHandler.getServlets();
            List<ServletHolder> newServlets = new ArrayList<>();
            newServlets.addAll(Arrays.asList(curServlets));
            if(!newServlets.remove(servlet)) {
                throw new IllegalArgumentException("Servlet not registered");
            }

            List<ServletMapping> newMappings = new ArrayList<>();
            newMappings.addAll(Arrays.asList(servletHandler.getServletMappings()));
            for(Iterator<ServletMapping> it = newMappings.iterator(); it.hasNext(); ) {
                ServletMapping m = it.next();
                if(servlet.getName().equals(m.getServletName())) {
                    for(String path : m.getPathSpecs()) {
                        registeredPaths.remove(path);
                    }
                    it.remove();
                    break;
                }
            }

            servletHandler.setServlets(newServlets.toArray(new ServletHolder[newServlets.size()]));
            servletHandler.setServletMappings(newMappings.toArray(new ServletMapping[newMappings.size()]));

            if(!servlet.isStopped()) {
                try {
                    servlet.stop();
                }
                catch(Exception e) {
                    throw new HttpConductorException(e);
                }
            }
        }
    }

    @Override
    public int getPort() {
        return port;
    }

    private static enum CsrfProtectionType {
        NONE,
        REFERER
    }

    private static enum AuthenticationType {
        NONE(null, null),
        BASIC(CredentialType.BASIC, BasicAuthenticator.class),
        DIGEST(CredentialType.DIGEST, DigestAuthenticator.class);

        public CredentialType getCredentialType() {
            return credentialType;
        }

        public Authenticator createAuthenticator() throws IllegalAccessException, InstantiationException {
            return authenticatorClass.newInstance();
        }

        private AuthenticationType(CredentialType credentialType, Class<? extends Authenticator> authenticatorClass) {
            this.credentialType = credentialType;
            this.authenticatorClass = authenticatorClass;
        }

        private final CredentialType credentialType;
        private final Class<? extends Authenticator> authenticatorClass;
    }

    private AuthenticationType safeParseAuthentication(String propName) {
        String propValue = configurationService.getProperty(propName);
        try {
            return AuthenticationType.valueOf(propValue.toUpperCase());
        } catch(IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid " + propName + " property: " + propValue);
        }
    }

    private CsrfProtectionType safeParseCsrfType(String propName) {
        String propValue = configurationService.getProperty(propName);
        try {
            return CsrfProtectionType.valueOf(propValue.toUpperCase());
        } catch(IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid " + propName + " property: " + propValue);
        }
    }

    private int safeParseInt(String propName) {
        String propValue = configurationService.getProperty(propName);
        try {
            return Integer.parseInt(propValue);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid " + propName + " property: " + propValue);
        }
    }

    @Override
    public void start() {
        String sslProperty = configurationService.getProperty(CONFIG_SSL_PROPERTY);

        int portLocal = safeParseInt(CONFIG_PORT_PROPERTY);
        String hostLocal = configurationService.getProperty(CONFIG_HOST_PROPERTY);
        int loginCacheSeconds = safeParseInt(CONFIG_LOGIN_CACHE_SECONDS);
        AuthenticationType login = safeParseAuthentication(CONFIG_LOGIN_PROPERTY);
        boolean sslOn = Boolean.parseBoolean(sslProperty);

        boolean crossOriginOn = Boolean.parseBoolean(configurationService.getProperty(CONFIG_XORIGIN_ENABLED));
        logger.info("Starting {} service on {}:{} with authentication {} and CORS {}",
                    new Object[] { sslOn ? "HTTPS" : "HTTP", hostLocal, portLocal, login, crossOriginOn ? "on" : "off"});
                    
        Server localServer = new Server();
        SelectChannelConnector connector;
        if(!sslOn) {
            connector = new SelectChannelConnectorExtended();
        } else {
            // Share keystore configuration with PSQL.
            SslContextFactory sslFactory = new SslContextFactory();
            sslFactory.setKeyStorePath(System.getProperty("javax.net.ssl.keyStore"));
            sslFactory.setKeyStorePassword(System.getProperty("javax.net.ssl.keyStorePassword"));
            connector = new SslSelectChannelConnectorExtended(sslFactory);
        }
        connector.setHost(hostLocal);
        connector.setPort(portLocal);
        connector.setThreadPool(new QueuedThreadPool(200));
        connector.setAcceptors(4);
        connector.setMaxIdleTime(300000);
        connector.setAcceptQueueSize(12000);
        connector.setLowResourcesConnections(25000);
        connector.setStatsOn(true);

        localServer.setConnectors(new Connector[]{connector});
        monitorService.registerServerMonitor(new ConnectionMonitor(connector));

        ServletContextHandler localRootContextHandler = new ServletContextHandler();
        localRootContextHandler.setContextPath("/");
        localServer.addBean(new JsonErrorHandler());

        try {
            if(login != AuthenticationType.NONE) {
                Authenticator authenticator = login.createAuthenticator();
                Constraint constraint = new Constraint(authenticator.getAuthMethod(), REST_ROLE);
                constraint.setAuthenticate(true);

                ConstraintMapping cm = new ConstraintMapping();
                cm.setPathSpec("/*");
                cm.setConstraint(constraint);

                ConstraintSecurityHandler sh =
                        crossOriginOn ? new CrossOriginConstraintSecurityHandler() : new ConstraintSecurityHandler();
                sh.setAuthenticator(authenticator);
                sh.setConstraintMappings(Collections.singletonList(cm));

                LoginService loginService =
                        new SecurityServiceLoginService(securityService, login.getCredentialType(), loginCacheSeconds,
                                configurationService.getProperty(CONFIG_REALM));
                sh.setLoginService(loginService);

                localRootContextHandler.setSecurityHandler(sh);
            }

            if (crossOriginOn) {
                addCrossOriginFilter(localRootContextHandler);
            }

            addCsrfFilter(localRootContextHandler);

            localServer.setHandler(localRootContextHandler);
            localServer.start();
        }
        catch (Exception e) {
            logger.error("failed to start HTTP server", e);
            throw new HttpConductorException(e);
        }

        synchronized (lock) {
            this.server = localServer;
            this.rootContextHandler = localRootContextHandler;
            this.registeredPaths = new HashSet<>();
            this.port = portLocal;
        }
    }

    @Override
    public void stop() {
        Server localServer;
        monitorService.deregisterServerMonitor(monitorService.getServerMonitors().get(SERVER_TYPE));
        synchronized (lock) {
            localServer = server;
            server = null;
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

    private void addCsrfFilter(ContextHandler handler) throws ServletException {
        CsrfProtectionType type = safeParseCsrfType(CONFIG_CSRF_TYPE);
        switch (type) {
            case NONE:
                break;
            case REFERER:
                FilterRegistration reg = handler.getServletContext().addFilter("CSRFFilter", CsrfProtectionRefererFilter.class);
                reg.addMappingForServletNames(null, false, "*");
                reg.setInitParameter(CsrfProtectionRefererFilter.ALLOWED_REFERERS_PARAM,
                        configurationService.getProperty(CONFIG_CSRF_ALLOWED_REFERERS));
                break;
            default:
                throw new IllegalArgumentException("Invalid " + CONFIG_CSRF_TYPE + " property: " + type);
        }
    }

    private void addCrossOriginFilter(ContextHandler handler) throws ServletException {
        FilterRegistration reg = handler.getServletContext().addFilter("CrossOriginFilter", CrossOriginFilter.class);
        reg.addMappingForServletNames(null, false, "*");
        reg.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM,
                             configurationService.getProperty(CONFIG_XORIGIN_ORIGINS));
        reg.setInitParameter(CrossOriginFilter.ALLOWED_METHODS_PARAM,
                             configurationService.getProperty(CONFIG_XORIGIN_METHODS));
        reg.setInitParameter(CrossOriginFilter.ALLOWED_HEADERS_PARAM,
                             configurationService.getProperty(CONFIG_XORIGIN_HEADERS));
        reg.setInitParameter(CrossOriginFilter.PREFLIGHT_MAX_AGE_PARAM,
                             configurationService.getProperty(CONFIG_XORIGIN_MAX_AGE));
        reg.setInitParameter(CrossOriginFilter.ALLOW_CREDENTIALS_PARAM,
                             configurationService.getProperty(CONFIG_XORIGIN_CREDENTIALS));
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

    private class ConnectionMonitor implements ServerMonitor {
        private final SelectChannelConnector connector;
        private final AtomicLong _statsStartedAt = new AtomicLong(System.currentTimeMillis());
        
        public ConnectionMonitor(SelectChannelConnector connector) {
            this.connector = connector;
        }
        
        @Override
        public String getServerType() {
            return SERVER_TYPE;
        }

        @Override
        public int getLocalPort() {
            return connector.getPort();
        }

        @Override
        public String getLocalHost() {
            return connector.getHost();
        }

        @Override
        public long getStartTimeMillis() {
            return _statsStartedAt.get();
        }

        @Override
        public int getSessionCount() {
            return connector.getConnections();
        }
    }
    
    private class SelectChannelConnectorExtended extends SelectChannelConnector {
        private Session session;
        @Override
        protected AsyncConnection newConnection(SocketChannel channel,final AsyncEndPoint endpoint)
        {
            AsyncHttpConnection conn = (AsyncHttpConnection)super.newConnection(channel, endpoint);
            ServerSessionMonitor sessionMonitor = new ServerSessionMonitor(SERVER_TYPE,
                    monitorService.allocateSessionId());

            conn.setAssociatedObject(sessionMonitor);
            this.session = sessionService.createSession();
            monitorService.registerSessionMonitor(sessionMonitor, session);
            return conn;
        }
        
        @Override  
        protected void connectionClosed(Connection connection) {
            if (connection instanceof AsyncHttpConnection) {
                AsyncHttpConnection conn = (AsyncHttpConnection)connection;
                ServerSessionMonitor monitor = (ServerSessionMonitor)conn.getAssociatedObject();
                if (monitor != null) {
                    monitorService.deregisterSessionMonitor(monitor, session);
                    conn.setAssociatedObject(null);
                }
            }
            super.connectionClosed(connection);
        }
    }
    
    private class SslSelectChannelConnectorExtended extends SslSelectChannelConnector {
        private Session session;
        public SslSelectChannelConnectorExtended(SslContextFactory sslFactory) {
            super(sslFactory);
        }

        @Override
        protected AsyncConnection newConnection(SocketChannel channel,final AsyncEndPoint endpoint)
        {
            AsyncHttpConnection conn = (AsyncHttpConnection)((SslConnection)super.newConnection(channel, endpoint)).getSslEndPoint().getConnection();
            ServerSessionMonitor sessionMonitor = new ServerSessionMonitor(SERVER_TYPE,
                    monitorService.allocateSessionId());

            conn.setAssociatedObject(sessionMonitor);
            this.session = sessionService.createSession();
            monitorService.registerSessionMonitor(sessionMonitor, session);
            return conn;
        }
        
        @Override  
        protected void connectionClosed (Connection connection) {
            if (connection instanceof SslConnection) {
                AsyncHttpConnection conn = (AsyncHttpConnection)((SslConnection) connection).getSslEndPoint().getConnection();
                ServerSessionMonitor monitor = (ServerSessionMonitor)conn.getAssociatedObject();
                if (monitor != null) {
                    monitorService.deregisterSessionMonitor(monitor, session);
                    conn.setAssociatedObject(null);
                }
            }
            super.connectionClosed(connection);
        }
    }
}
