
package com.akiban.server.service.security;

import com.akiban.server.service.session.Session;

import javax.servlet.http.HttpServletRequest;

import java.util.Collection;

public interface SecurityService
{
    public static final String REALM = "AkServer";
    public static final String ADMIN_ROLE = "admin";

    public static final Session.Key<User> SESSION_KEY = 
        Session.Key.named("SECURITY_USER");

    public User authenticate(Session session, String name, String password);
    public User authenticate(Session session, String name, String password, byte[] salt);

    public boolean isAccessible(Session session, String schema);
    public boolean isAccessible(HttpServletRequest request, String schema);

    public void addRole(String name);
    public void deleteRole(String name);
    public User getUser(String name);
    public User addUser(String name, String password, Collection<String> roles);
    public void deleteUser(String name);
    public void changeUserPassword(String name, String password);
    public void clearAll(Session session);
}
