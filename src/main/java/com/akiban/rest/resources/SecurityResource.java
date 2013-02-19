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

import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.service.security.User;
import com.akiban.server.service.security.SecurityService;
import com.google.inject.Inject;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Security operations via REST.
 */
@Path("/security")
public class SecurityResource {
    @Inject
    private SecurityService securityService;

    @Path("/users")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addUser(@Context HttpServletRequest request,
                            byte[] userBytes) throws Exception {
        JsonNode node = new ObjectMapper().readTree(userBytes);
        if (!request.isUserInRole(SecurityService.ADMIN_ROLE)) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        JsonNode userNode = node.get("user");
        JsonNode passwordNode = node.get("password");
        JsonNode rolesNode = node.get("roles");
        if ((userNode == null) || !userNode.isTextual()) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(jsonError(ErrorCode.SECURITY, "user string required"))
                .build();
        }
        if ((passwordNode == null) || !passwordNode.isTextual()) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(jsonError(ErrorCode.SECURITY, "password string required"))
                .build();
        }
        if ((rolesNode == null) || !rolesNode.isArray()) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(jsonError(ErrorCode.SECURITY, "roles array required"))
                .build();
        }
        String user = userNode.getValueAsText();
        String password = passwordNode.getValueAsText();
        List<String> roles = new ArrayList<>();
        for (JsonNode elem : rolesNode) {
            roles.add(elem.getValueAsText());
        }
        User newUser;
        try {
            newUser = securityService.addUser(user, password, roles);
        }
        catch (InvalidOperationException ex) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(jsonError(ex.getCode(), ex.getMessage()))
                .build();
        }
        return Response.status(Response.Status.OK)
            .entity(String.format("{\"id\":%d}", newUser.getId()))
            .build();
    }

    private String jsonError(ErrorCode code, String message) {
        return String.format("{\"code\":\"%s\", \"message\":\"%s\"}",
                             code.getFormattedValue(), message);
    }
}
