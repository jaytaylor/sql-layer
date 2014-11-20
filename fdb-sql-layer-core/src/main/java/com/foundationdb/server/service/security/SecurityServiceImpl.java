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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.AuthenticationFailedException;
import com.foundationdb.server.error.SecurityException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.monitor.MonitorService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.sql.server.ServerCallContextStack;
import com.foundationdb.sql.server.ServerQueryContext;
import com.foundationdb.util.Strings;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class SecurityServiceImpl implements SecurityService, Service {
    public static final String SCHEMA = TableName.SECURITY_SCHEMA;
    public static final String ROLES_TABLE_NAME = "roles";
    public static final String USERS_TABLE_NAME = "users";
    public static final String USER_ROLES_TABLE_NAME = "user_roles";
    public static final String ADD_ROLE_PROC_NAME = "add_role";
    public static final String ADD_USER_PROC_NAME = "add_user";
    public static final int TABLE_VERSION = 1;

    public static final String ADMIN_USER_NAME = "foundationdb";
    public static final String CONNECTION_URL = "jdbc:default:connection";

    public static final String ADD_ROLE_SQL = "INSERT INTO roles(name) VALUES(?)";
    public static final String DELETE_ROLE_SQL = "DELETE FROM roles WHERE name = ?";
    public static final String GET_USER_SQL = "SELECT id, name, password_basic, password_digest, password_md5, (SELECT r.id, r.name FROM roles r INNER JOIN user_roles ur ON r.id = ur.role_id WHERE ur.user_id = users.id) FROM users WHERE name = ?";
    public static final String ADD_USER_SQL = "INSERT INTO users(name, password_basic, password_digest, password_md5) VALUES(?,?,?,?) RETURNING id";
    public static final String ADD_USER_ROLE_SQL = "INSERT INTO user_roles(user_id, role_id) VALUES(?,(SELECT id FROM roles WHERE name = ?))";
    public static final String CHANGE_USER_PASSWORD_SQL = "UPDATE users SET password_basic = ?, password_digest = ?, password_md5 = ? WHERE name = ?";
    public static final String DELETE_USER_SQL = "DELETE FROM users WHERE name = ?";
    public static final String DELETE_ROLE_USER_ROLES_SQL = "DELETE FROM user_roles WHERE role_id IN (SELECT id FROM roles WHERE name = ?)";
    public static final String DELETE_USER_USER_ROLES_SQL = "DELETE FROM user_roles WHERE user_id IN (SELECT id FROM users WHERE name = ?)";
    
    public static final String RESTRICT_USER_SCHEMA_PROPERTY = "fdbsql.restrict_user_schema";
    public static final String HTTP_REALM_PROPERTY = "fdbsql.http.realm"; // See also HttpConductorImpl

    private final ConfigurationService configService;
    private final SchemaManager schemaManager;
    private final MonitorService monitor;

    private boolean restrictUserSchema;
    private String httpRealm;

    private static final Logger logger = LoggerFactory.getLogger(SecurityServiceImpl.class);

    @Inject
    public SecurityServiceImpl(ConfigurationService configService,
                               SchemaManager schemaManager,
                               MonitorService monitor) {
        this.configService = configService;
        this.schemaManager = schemaManager;
        this.monitor = monitor;
    }

    // Connections are not thread safe, and prepared statements remember a Session,
    // so rather than trying to pool them, just make a new one each
    // request, which is reasonably cheap.
    protected Connection openConnection() throws SQLException {
        Properties info = new Properties();
        info.put("user", ADMIN_USER_NAME);
        info.put("password", "");
        info.put("database", SCHEMA);
        Connection conn = DriverManager.getConnection(CONNECTION_URL, info);
        conn.setAutoCommit(false);
        return conn;
    }

    protected void cleanup(Connection conn, Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            }
            catch (SQLException ex) {
                logger.warn("Error closing statement", ex);
            }
        }
        if (conn != null) {
            try {
                conn.close();
            }
            catch (SQLException ex) {
                logger.warn("Error closing connection", ex);
            }
        }
    }

    /* SecurityService */

    @Override
    public void addRole(String name) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = openConnection();
            stmt = conn.prepareStatement(ADD_ROLE_SQL);
            stmt.setString(1, name);
            int nrows = stmt.executeUpdate();
            if (nrows != 1) {
                throw new SecurityException("Failed to add role " + name);
            }
            conn.commit();
        }
        catch (SQLException ex) {
            throw new SecurityException("Error adding role", ex);
        }
        finally {
            cleanup(conn, stmt);

        }
    }

    @Override
    public void deleteRole(String name) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = openConnection();
            stmt = conn.prepareStatement(DELETE_ROLE_SQL);
            stmt.setString(1, name);
            int nrows = stmt.executeUpdate();
            if (nrows != 1) {
                throw new SecurityException("Failed to delete role");
            }
            conn.commit();
        }
        catch (SQLException ex) {
            throw new SecurityException("Error deleting role", ex);
        }
        finally {
            cleanup(conn, stmt);
        }
    }

    @Override
    public User getUser(String name) {
        User user = null;
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = openConnection();
            stmt = conn.prepareStatement(GET_USER_SQL);
            stmt.setString(1, name);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                user = getUser(rs);
            }
            rs.close();
            conn.commit();
        }
        catch (SQLException ex) {
            throw new SecurityException("Error adding role", ex);
        }
        finally {
            cleanup(conn, stmt);
        }
        return user;
    }

    protected User getUser(ResultSet rs) throws SQLException {
        List<String> roles = new ArrayList<>();
        ResultSet rs1 = (ResultSet)rs.getObject(6);
        while (rs1.next()) {
            roles.add(rs1.getString(2));
        }
        rs1.close();
        return new User(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getString(4), roles);
    }

    @Override
    public User addUser(String name, String password, Collection<String> roles) {
        int id;
        Connection conn = null;
        PreparedStatement stmt = null;
        String basicPassword = basicPassword(password);
        String digestPassword = digestPassword(name, password);
        try {
            conn = openConnection();
            stmt = conn.prepareStatement(ADD_USER_SQL);
            stmt.setString(1, name);
            stmt.setString(2, basicPassword);
            stmt.setString(3, digestPassword);
            stmt.setString(4, md5Password(name, password));
            int nrows = stmt.executeUpdate();
            if (nrows != 1) {
                throw new SecurityException("Failed to add user " + name);
            }
            ResultSet rs = stmt.getGeneratedKeys();
            if (rs.next()) {
                id = rs.getInt(1);
            }
            else {
                throw new SecurityException("Failed to get user id for " + name);
            }
            rs.close();
            stmt.close();
            stmt = null;
            stmt = conn.prepareStatement(ADD_USER_ROLE_SQL);
            stmt.setInt(1, id);
            for (String role : roles) {
                stmt.setString(2, role);
                nrows = stmt.executeUpdate();
                if (nrows != 1) {
                    throw new SecurityException("Failed to add role " + role);
                }
            }
            conn.commit();
        }
        catch (SQLException ex) {
            throw new SecurityException("Error adding user", ex);
        }
        finally {
            cleanup(conn, stmt);
        }
        return new User(id, name, basicPassword, digestPassword, new ArrayList<>(roles));
    }

    @Override
    public void deleteUser(String name) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = openConnection();
            stmt = conn.prepareStatement(DELETE_USER_USER_ROLES_SQL);
            stmt.setString(1, name);
            stmt.executeUpdate();
            stmt.close();
            stmt = null;
            stmt = conn.prepareStatement(DELETE_USER_SQL);
            stmt.setString(1, name);
            int nrows = stmt.executeUpdate();
            if (nrows != 1) {
                throw new SecurityException("Failed to delete user");
            }
            conn.commit();
            monitor.deregisterUserMonitor(name);
        }
        catch (SQLException ex) {
            throw new SecurityException("Error deleting user", ex);
        }
        finally {
            cleanup(conn, stmt);
        }
    }

    @Override
    public void changeUserPassword(String name, String password) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = openConnection();
            stmt = conn.prepareStatement(CHANGE_USER_PASSWORD_SQL);
            stmt.setString(1, basicPassword(password));
            stmt.setString(2, digestPassword(name, password));
            stmt.setString(3, md5Password(name, password));
            stmt.setString(4, name);
            int nrows = stmt.executeUpdate();
            if (nrows != 1) {
                throw new SecurityException("Failed to change user");
            }
            conn.commit();
        }
        catch (SQLException ex) {
            throw new SecurityException("Error changing user", ex);
        }
        finally {
            cleanup(conn, stmt);
        }
    }

    @Override
    public User authenticate(Session session, String name, String password) {
        String expected = md5Password(name, password);
        User user = null;
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = openConnection();
            stmt = conn.prepareStatement(GET_USER_SQL);
            stmt.setString(1, name);
            ResultSet rs = stmt.executeQuery();
            if (rs.next() && expected.equals(rs.getString(5))) {
                user = getUser(rs);
            }
            rs.close();
            conn.commit();
        }
        catch (SQLException ex) {
            throw new SecurityException("Error adding role", ex);
        }
        finally {
            cleanup(conn, stmt);
        }
        if (user == null) {
            throw new AuthenticationFailedException("invalid username or password");
        }
        if (session != null) {
            session.put(SESSION_KEY, user);
        }
        if (monitor.getUserMonitor(user.getName()) == null) {
            monitor.registerUserMonitor(new UserMonitorImpl(user.getName()));
        }
        return user;
    }

    @Override
    public User authenticate(Session session, String name, String password, byte[] salt) {
        User user = null;
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = openConnection();
            stmt = conn.prepareStatement(GET_USER_SQL);
            stmt.setString(1, name);
            ResultSet rs = stmt.executeQuery();
            if (rs.next() && password.equals(salted(rs.getString(5), salt))) {
                user = getUser(rs);
            }
            rs.close();
            conn.commit();
        }
        catch (SQLException ex) {
            throw new SecurityException("Error adding role", ex);
        }
        finally {
            cleanup(conn, stmt);
        }
        if (user == null) {
            throw new AuthenticationFailedException("invalid username or password");
        }
        if (session != null) {
            session.put(SESSION_KEY, user);
        }
        if (monitor.getUserMonitor(user.getName()) == null) {
            monitor.registerUserMonitor(new UserMonitorImpl(user.getName()));
        }
        return user;
    }

    protected String basicPassword(String password) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(password.getBytes("UTF-8"));
            return formatMD5(md.digest(), true);
        }
        catch (NoSuchAlgorithmException ex) {
            throw new AkibanInternalException("Cannot create digest", ex);
        }
        catch (UnsupportedEncodingException ex) {
            throw new AkibanInternalException("Cannot create digest", ex);
        }
    }

    protected String digestPassword(String user, String password) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(user.getBytes("UTF-8"));
            md.update((":" + httpRealm + ":").getBytes("UTF-8"));
            md.update(password.getBytes("UTF-8"));
            return formatMD5(md.digest(), true);
        }
        catch (NoSuchAlgorithmException ex) {
            throw new AkibanInternalException("Cannot create digest", ex);
        }
        catch (UnsupportedEncodingException ex) {
            throw new AkibanInternalException("Cannot create digest", ex);
        }
    }

    protected String md5Password(String user, String password) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(password.getBytes("UTF-8"));
            md.update(user.getBytes("UTF-8"));
            return formatMD5(md.digest(), false);
        }
        catch (NoSuchAlgorithmException ex) {
            throw new AkibanInternalException("Cannot create digest", ex);
        }
        catch (UnsupportedEncodingException ex) {
            throw new AkibanInternalException("Cannot create digest", ex);
        }
    }

    protected String salted(String base, byte[] salt) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(base.getBytes("UTF-8"), 3, 32); // Skipping "md5".
            md.update(salt);
            return formatMD5(md.digest(), false);
        }
        catch (NoSuchAlgorithmException ex) {
            throw new AkibanInternalException("Cannot create digest", ex);
        }
        catch (UnsupportedEncodingException ex) {
            throw new AkibanInternalException("Cannot create digest", ex);
        }
    }

    protected String formatMD5(byte[] md5, boolean forDigest) {
        StringBuilder str = new StringBuilder();
        str.append(forDigest ? "MD5:" : "md5");
        // Strings#formatMD5 wants toLowerCase for second parameter, inverse of the forDigest flag
        str.append(Strings.formatMD5(md5, !forDigest));
        return str.toString();
    }

    @Override
    public void clearAll(Session session) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = openConnection();
            stmt = conn.createStatement();
            stmt.execute("DELETE FROM user_roles");
            stmt.execute("DELETE FROM users");
            stmt.execute("DELETE FROM roles");
            conn.commit();
        }
        catch (SQLException ex) {
            throw new SecurityException("Error adding role", ex);
        }
        finally {
            cleanup(conn, stmt);
        }
        session.remove(SESSION_KEY);
    }

    @Override
    /** If this session is authenticated, does it have access to the given schema?
     *
     * NOTE: If authentication is enabled, caller must not call this (that is, allow
     * any queries) without authentication, since that is indistinguishable from
     * authentication disabled.
     *
     * @see com.foundationdb.sql.pg.PostgresServerConnection#authenticationOkay
     */
    public boolean isAccessible(Session session, String schema) {
        User user = session.get(SESSION_KEY);
        if (user == null) return true; // Authentication disabled.
        return isAccessible(user.getName(), schema) || user.hasRole(ADMIN_ROLE);
    }

    @Override
    /** If this request is authenticated, does it have access to the given schema?
     *
     * NOTE: If authentication is enabled, caller must not call this (that is, allow
     * any queries) without authentication, since that is indistinguishable from
     * authentication disabled.
     *
     * @see com.foundationdb.http.HttpConductorImpl.AuthenticationType
     */
    public boolean isAccessible(HttpServletRequest request, String schema) {
        Principal user = request.getUserPrincipal();
        if (user == null) return true; // Authentication disabled.
        return isAccessible(user.getName(), schema) || request.isUserInRole(ADMIN_ROLE);
    }

    protected boolean isAccessible(String user, String schema) {
        return !restrictUserSchema ||
            user.equals(schema) ||
            TableName.INFORMATION_SCHEMA.equals(schema) ||
            TableName.SQLJ_SCHEMA.equals(schema) ||
            TableName.SYS_SCHEMA.equals(schema);
    }

    @Override
    /** If this session is authenticated, does it administrative access?
     */
    public boolean hasRestrictedAccess(Session session) {
        User user = session.get(SESSION_KEY);
        if (user == null) return true; // Authentication disabled.
        return user.hasRole(ADMIN_ROLE);
    }

    /* Service */
    
    @Override
    public void start() {
        restrictUserSchema = Boolean.parseBoolean(configService.getProperty(RESTRICT_USER_SCHEMA_PROPERTY));
        httpRealm = configService.getProperty(HTTP_REALM_PROPERTY);
        registerSystemObjects();
        if (restrictUserSchema) {
            schemaManager.setSecurityService(this); // Injection would be circular.
        }
    }

    @Override
    public void stop() {
        deregisterSystemObjects();
    }

    @Override
    public void crash() {
        stop();
    }

    protected void registerSystemObjects() {
        AkibanInformationSchema ais = buildSystemObjects();
        schemaManager.registerStoredInformationSchemaTable(ais.getTable(SCHEMA, ROLES_TABLE_NAME), TABLE_VERSION);
        schemaManager.registerStoredInformationSchemaTable(ais.getTable(SCHEMA, USERS_TABLE_NAME), TABLE_VERSION);
        schemaManager.registerStoredInformationSchemaTable(ais.getTable(SCHEMA, USER_ROLES_TABLE_NAME), TABLE_VERSION);
        schemaManager.registerSystemRoutine(ais.getRoutine(SCHEMA, ADD_ROLE_PROC_NAME));
        schemaManager.registerSystemRoutine(ais.getRoutine(SCHEMA, ADD_USER_PROC_NAME));
    }

    protected void deregisterSystemObjects() {
        schemaManager.unRegisterSystemRoutine(new TableName(SCHEMA, ADD_ROLE_PROC_NAME));
        schemaManager.unRegisterSystemRoutine(new TableName(SCHEMA, ADD_USER_PROC_NAME));
    }

    protected AkibanInformationSchema buildSystemObjects() {
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA, schemaManager.getTypesTranslator());
        builder.table(ROLES_TABLE_NAME)
            .autoIncInt("id", 1)
            .colString("name", 128, false)
            .pk("id")
            .uniqueKey("role_name", "name");
        builder.table(USERS_TABLE_NAME)
            .autoIncInt("id", 1)
            .colString("name", 128, false)
            .colString("password_basic", 36, true)
            .colString("password_digest", 36, true)
            .colString("password_md5", 35, true)
            .pk("id")
            .uniqueKey("user_name", "name");
        builder.table(USER_ROLES_TABLE_NAME)
            .autoIncInt("id", 1)
            .colInt("role_id", false)
            .colInt("user_id", false)
            .pk("id")
            .uniqueKey("user_roles", "user_id", "role_id")
            .joinTo(USERS_TABLE_NAME)
            .on("user_id", "id");
        builder.procedure(ADD_ROLE_PROC_NAME)
            .language("java", Routine.CallingConvention.JAVA)
            .paramStringIn("role_name", 128)
            .externalName(Routines.class.getName(), "addRole");
        builder.procedure(ADD_USER_PROC_NAME)
            .language("java", Routine.CallingConvention.JAVA)
            .paramStringIn("user_name", 128)
            .paramStringIn("password", 128)
            .paramStringIn("roles", 128)
            .externalName(Routines.class.getName(), "addUser");
        return builder.ais(true);
    }

    // TODO: Temporary way of accessing these via stored procedures.
    public static class Routines {
        public static void addRole(String roleName) {
            ServerQueryContext context = ServerCallContextStack.getCallingContext();
            SecurityService service = context.getServer().getSecurityService();
            service.addRole(roleName);
        }

        public static void addUser(String userName, String password, String roles) {
            ServerQueryContext context = ServerCallContextStack.getCallingContext();
            SecurityService service = context.getServer().getSecurityService();
            service.addUser(userName, password, Arrays.asList(roles.split(",")));
        }
    }

}
