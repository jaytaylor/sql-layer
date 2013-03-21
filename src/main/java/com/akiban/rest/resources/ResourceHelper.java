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

package com.akiban.rest.resources;

import com.akiban.ais.model.TableName;
import com.akiban.server.service.security.SecurityService;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.security.Principal;

public class ResourceHelper {
    public static final Response FORBIDDEN_RESPONSE = Response.status(Response.Status.FORBIDDEN).build();

    // Standard but not otherwise defined
    public static final String APPLICATION_JAVASCRIPT = "application/javascript";
    public static final MediaType APPLICATION_JAVASCRIPT_TYPE = MediaType.valueOf(APPLICATION_JAVASCRIPT);

    // For @Produces argument
    public static final String MEDIATYPE_JSON_JAVASCRIPT = MediaType.APPLICATION_JSON + "," + APPLICATION_JAVASCRIPT;

    public static final String JSONP_ARG_NAME = "callback";

    public static String getSchema(HttpServletRequest request) {
        Principal user = request.getUserPrincipal();
        return (user == null) ? "" : user.getName();
    }

    public static TableName parseTableName(HttpServletRequest request, String name) {
        String schema = getSchema(request);
        return TableName.parse(schema, name);
    }

    public static void checkTableAccessible(SecurityService security, HttpServletRequest request, TableName name) {
        checkSchemaAccessible(security, request, name.getSchemaName());
    }

    public static void checkSchemaAccessible(SecurityService security, HttpServletRequest request, String schema) {
        if(!security.isAccessible(request, schema)) {
            throw new WebApplicationException(FORBIDDEN_RESPONSE);
        }
    }
}
