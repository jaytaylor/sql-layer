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

import com.akiban.rest.ResourceRequirements;
import com.akiban.rest.RestResponseBuilder;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.service.security.SecurityService;
import com.akiban.server.service.security.User;
import com.akiban.server.service.session.Session;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import static com.akiban.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;

/**
 * Security operations via REST.
 */
@Path("/security")
public class SecurityResource {
    private final ResourceRequirements reqs;

    public SecurityResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    @Path("/users")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response addUser(@Context HttpServletRequest request,
                            byte[] userBytes) throws Exception {
        RestResponseBuilder response = RestResponseBuilder.forRequest(request);
        if (!request.isUserInRole(SecurityService.ADMIN_ROLE)) {
            return response.status(Response.Status.FORBIDDEN).build();
        }
        JsonNode node = new ObjectMapper().readTree(userBytes);
        JsonNode userNode = node.get("user");
        JsonNode passwordNode = node.get("password");
        JsonNode rolesNode = node.get("roles");
        if ((userNode == null) || !userNode.isTextual()) {
            return badRequest(response, "user string required");
        }
        if ((passwordNode == null) || !passwordNode.isTextual()) {
            return badRequest(response, "password string required");
        }
        if ((rolesNode == null) || !rolesNode.isArray()) {
            return badRequest(response, "roles array required");
        }
        final String user = userNode.getValueAsText();
        final String password = passwordNode.getValueAsText();
        final List<String> roles = new ArrayList<>();
        for (JsonNode elem : rolesNode) {
            roles.add(elem.getValueAsText());
        }
        response.body(new RestResponseBuilder.BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                User newUser = reqs.securityService.addUser(user, password, roles);
                writer.write("{\"id\":");
                writer.print(newUser.getId());
                writer.write('}');
            }
        });
        return response.build();
    }

    @Path("/users/{user}")
    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response deleteUser(@Context HttpServletRequest request,
                               @PathParam("user") final String user) {
        RestResponseBuilder response = RestResponseBuilder.forRequest(request);
        if (!request.isUserInRole(SecurityService.ADMIN_ROLE)) {
            return response.status(Response.Status.FORBIDDEN).build();
        }
        response.body(new RestResponseBuilder.BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                try (Session session = reqs.sessionService.createSession()) {
                    reqs.dxlService.ddlFunctions().dropSchema(session, user);
                    reqs.securityService.deleteUser(user);
                }
            }
        });
        return response.build();
    }

    private static Response badRequest(RestResponseBuilder builder, String message) {
        return builder
                .status(Response.Status.BAD_REQUEST)
                .body(ErrorCode.SECURITY, message)
                .build();
    }
}
