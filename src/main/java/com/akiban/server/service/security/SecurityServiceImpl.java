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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Routine;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.store.SchemaManager;
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
    public static final String SCHEMA = TableName.SECURITY_SCHEMA;
    public static final String ROLES_TABLE_NAME = "roles";
    public static final String USERS_TABLE_NAME = "users";
    public static final String USER_ROLES_TABLE_NAME = "user_roles";
    public static final String ADD_ROLE_PROC_NAME = "add_role";
    public static final String ADD_USER_PROC_NAME = "add_user";
    public static final int TABLE_VERSION = 1;

    private final ConfigurationService configService;
    private final EmbeddedJDBCService jdbcService;
    private final SchemaManager schemaManager;

    private Connection connection;

    private static final Logger logger = LoggerFactory.getLogger(SecurityServiceImpl.class);

    @Inject
    public SecurityServiceImpl(ConfigurationService configService,
                               EmbeddedJDBCService jdbcService,
                               SchemaManager schemaManager) {
        this.configService = configService;
        this.jdbcService = jdbcService;
        this.schemaManager = schemaManager;
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
        registerSystemObjects();
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

    protected void registerSystemObjects() {
        AkibanInformationSchema ais = buildSystemObjects();
        schemaManager.registerStoredInformationSchemaTable(ais.getUserTable(SCHEMA, ROLES_TABLE_NAME), TABLE_VERSION);
        schemaManager.registerStoredInformationSchemaTable(ais.getUserTable(SCHEMA, USERS_TABLE_NAME), TABLE_VERSION);
        schemaManager.registerStoredInformationSchemaTable(ais.getUserTable(SCHEMA, USER_ROLES_TABLE_NAME), TABLE_VERSION);
        schemaManager.registerSystemRoutine(ais.getRoutine(SCHEMA, ADD_ROLE_PROC_NAME));
        schemaManager.registerSystemRoutine(ais.getRoutine(SCHEMA, ADD_USER_PROC_NAME));
    }

    protected AkibanInformationSchema buildSystemObjects() {
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA);
        builder.userTable(ROLES_TABLE_NAME)
            .autoIncLong("id", 1)
            .colString("name", 128, false)
            .pk("id");
        builder.userTable(USERS_TABLE_NAME)
            .autoIncLong("id", 1)
            .colString("name", 128, false)
            .pk("id");
        builder.userTable(USER_ROLES_TABLE_NAME)
            .autoIncLong("id", 1)
            .colLong("role_id", false)
            .colLong("user_id", false)
            .pk("id")
            .joinTo(USERS_TABLE_NAME)
            .on("user_id", "id");
        builder.procedure(ADD_ROLE_PROC_NAME)
            .language("java", Routine.CallingConvention.JAVA)
            .paramStringIn("role_name", 128)
            .paramLongOut("role_id")
            .externalName(Routines.class.getName(), "addRole");
        builder.procedure(ADD_USER_PROC_NAME)
            .language("java", Routine.CallingConvention.JAVA)
            .paramStringIn("user_name", 128)
            .paramStringIn("password", 128)
            .paramStringIn("roles", 128)
            .paramLongOut("user_id")
            .externalName(Routines.class.getName(), "addUser");
        return builder.ais(true);
    }

    static class Routines {
        public static void addRole(String roleName, long[] roleId) {
        }

        public static void addUser(String roleName, long[] roleId) {
        }
    }

}
