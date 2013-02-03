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

import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.sql.embedded.EmbeddedJDBCService;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;

public class SecurityServiceImpl implements SecurityService, Service {
    private final ConfigurationService configService;
    private final EmbeddedJDBCService jdbcService;
    
    private Connection connection;

    private static final Logger logger = LoggerFactory.getLogger(SecurityServiceImpl.class);

    @Inject
    public SecurityServiceImpl(ConfigurationService configService,
                               EmbeddedJDBCService jdbcService) {
        this.configService = configService;
        this.jdbcService = jdbcService;
    }

    /* SecurityService */

    @Override
    public User addUser(String name, String password, Collection<String> roles) {
        return null;
    }

    @Override
    public User authenticate(String name, String password) {
        return null;
    }

    @Override
    public User authenticate(String name, String password, byte[] salt) {
        return null;
    }

    /* Service */
    
    @Override
    public void start() {
    }

    @Override
    public void stop() {
        if (connection != null) {
            try {
                connection.close();
            }
            catch (SQLException ex) {
                logger.warn("Error closing connection", ex);
            }
            connection = null;
        }
    }

    @Override
    public void crash() {
        stop();
    }

}
