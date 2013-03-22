
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
