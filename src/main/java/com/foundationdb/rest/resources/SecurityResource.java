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

package com.foundationdb.rest.resources;

import com.foundationdb.rest.ResourceRequirements;
import com.foundationdb.rest.RestResponseBuilder;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.security.User;
import com.foundationdb.server.service.session.Session;

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

import com.fasterxml.jackson.databind.JsonNode;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import static com.foundationdb.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;
import static com.foundationdb.util.JsonUtils.readTree;

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
        JsonNode node = readTree(userBytes);
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
        final String user = userNode.asText();
        final String password = passwordNode.asText();
        final List<String> roles = new ArrayList<>();
        for (JsonNode elem : rolesNode) {
            roles.add(elem.asText());
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
