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

package com.akiban.admin;

import java.io.IOException;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.mortbay.jetty.handler.AbstractHandler;

import com.akiban.admin.action.ClearConfig;
import com.akiban.admin.action.StartChunkservers;
import com.akiban.admin.action.StopChunkservers;

class AdminHTTPHandler extends AbstractHandler
{
    public void handle(String key,
                       HttpServletRequest request,
                       HttpServletResponse response,
                       int dispatch) throws IOException
    {
        String method = request.getMethod();
        try {
            if (method.equals(GET)) {
                handleGet(key, request, response);
            } else if (method.equals(PUT)) {
                handlePut(key, request, response);
            } else {
                returnError(response, String.format("Unsupported method: %s", method));
            }
        } catch (Admin.BadKeyException e) {
            returnError(response, String.format("Key error: %s", key));
        } catch (Exception e) {
            String message = String.format("Caught %s on request %s: %s", e.getClass().getName(), key, e.getMessage());
            logger.error(message, e);
            returnError(response, message);
        }
    }

    private void handleGet(String key,
                           HttpServletRequest request,
                           HttpServletResponse response) throws IOException
    {
        if (key.startsWith("/config/") || key.startsWith("/state/")) {
            handleGetCluster(key, request, response);
        } else if (key.equals("/action")) {
            handleGetAction(key, request, response);
        } else {
            returnError(response, String.format("Unrecognized request: %s", key));
        }
    }

    private void handleGetCluster(String key, HttpServletRequest request, HttpServletResponse response)
        throws IOException
    {
        Admin admin = Admin.only();
        AdminValue value = admin.get(key);
        returnOK(response, value.value());
    }

    private void handleGetAction(String key, HttpServletRequest request, HttpServletResponse response)
        throws IOException
    {
        String op = request.getParameter(OP);
        try {
            synchronized (GLOBAL_ADMIN_LOCK) {
                if (op.equals(START)) {
                    StartChunkservers.only().run();
                    returnOK(response);
                } else if (op.equals(STOP)) {
                    StopChunkservers.only().run();
                    returnOK(response);
                } else if (op.equals(CLEAR)) {
                    ClearConfig.only().run();
                    returnOK(response);
                } else {
                    returnError(response, String.format("Unknown action: %s", op));
                }
            }
        } catch (Exception e) {
            String message = String.format("Caught %s on %s %s", e.getClass().getName(), key, op);
            logger.warn(message, e);
            returnError(response, message);
        }
    }

    private void handlePut(String key,
                           HttpServletRequest request,
                           HttpServletResponse response) throws IOException
    {
        Admin admin = Admin.only();
        ServletInputStream input = request.getInputStream();
        byte[] bytes = new byte[MAX_VALUE_SIZE];
        int bytesRead = input.read(bytes);
        if (bytesRead < 0) {
            returnError(response, String.format("EOF on reading input in PUT request for %s", key));
        } else {
            AdminValue adminValue = admin.get(key);
            String newValue = new String(bytes, 0, bytesRead);
            try {
                admin.set(key, adminValue.version(), newValue);
            } catch (Admin.StaleUpdateException e) {
                returnError(response, String.format("PUT of %s failed due to concurrent update. Try again.", key));
            }
            returnOK(response);
        }
    }

    private void returnError(HttpServletResponse response,
                             String errorMessage) throws IOException
    {
        response.setContentType("text/plain");
        response.getWriter().println(errorMessage);
        response.getWriter().close();
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    }

    private void returnOK(HttpServletResponse response) throws IOException
    {
        returnOK(response, "OK");
    }

    private void returnOK(HttpServletResponse response, String message) throws IOException
    {
        response.setContentType("text/plain");
        response.getWriter().println(message);
        response.getWriter().close();
        response.setStatus(HttpServletResponse.SC_OK);
    }

    // ZooKeeper promises to support "1M" of data. Not sure if that's 10**6 or 2**10, so let's go with
    // the smaller number.
    private static final int MAX_VALUE_SIZE = 1000 * 1000;
    private static final Object GLOBAL_ADMIN_LOCK = new Object();
    private static final Logger logger = LoggerFactory.getLogger(AdminHTTPHandler.class);

    private static final String GET = "GET";
    private static final String PUT = "PUT";
    private static final String OP = "op";
    private static final String START = "start";
    private static final String STOP = "stop";
    private static final String CLEAR = "clear";
}
