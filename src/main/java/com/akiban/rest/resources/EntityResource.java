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

import com.akiban.ais.AISCloner;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.protobuf.ProtobufWriter;
import com.akiban.rest.ResourceRequirements;
import com.akiban.server.entity.changes.DDLBasedSpaceModifier;
import com.akiban.server.entity.changes.SpaceDiff;
import com.akiban.server.entity.fromais.AisToSpace;
import com.akiban.server.entity.model.Space;
import com.akiban.server.entity.model.diff.JsonDiffPreview;
import com.akiban.server.service.session.Session;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.Principal;

@Path("/entity")
public final class EntityResource {
    private static final Response FORBIDDEN = Response.status(Response.Status.FORBIDDEN).build();

    private final ResourceRequirements reqs;

    public EntityResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSpace(@Context HttpServletRequest request,
                             @QueryParam("space") String schema) {
        if(schema == null) {
            schema = getUserSchema(request);
        }
        if (schema == null || !reqs.securityService.isAccessible(request, schema)) {
            return FORBIDDEN;
        }
        try (Session session = reqs.sessionService.createSession()) {
            reqs.transactionService.beginTransaction(session);
            try {
                AkibanInformationSchema ais = reqs.dxlService.ddlFunctions().getAIS(session);
                ais = AISCloner.clone(ais, new ProtobufWriter.SingleSchemaSelector(schema));
                Space space = AisToSpace.create(ais);
                String json = space.toJson() + "\n";
                return Response.status(Response.Status.OK).entity(json).build();
            }
            finally {
                reqs.transactionService.commitTransaction(session);
            }
        }
    }

    @POST
    @Path("/preview")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response preview(@Context HttpServletRequest request,
                            final InputStream postInput) throws IOException {
        return preview(request, getUserSchema(request), postInput);
    }

    @POST
    @Path("/preview/{schema}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response preview(@Context HttpServletRequest request,
                            @PathParam("schema") String schema,
                            final InputStream postInput) throws IOException {
        return previewOrApply(request, schema, postInput, false);
    }

    @POST
    @Path("/apply")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response apply(@Context HttpServletRequest request,
                          final InputStream postInput) throws IOException {
        return apply(request, getUserSchema(request), postInput);
    }

    @POST
    @Path("/apply/{schema}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response apply(@Context HttpServletRequest request,
                          @PathParam("schema") String schema,
                          final InputStream postInput) throws IOException {
        return previewOrApply(request, schema, postInput, true);
    }

    private Response previewOrApply(HttpServletRequest request, String schema, InputStream postInput, boolean doApply) throws IOException {
        if (schema == null || !reqs.securityService.isAccessible(request, schema)) {
            return FORBIDDEN;
        }
        try (Session session = reqs.sessionService.createSession()) {
            // Cannot have transaction when attempting to perform DDL
            AkibanInformationSchema ais = reqs.dxlService.ddlFunctions().getAIS(session);
            ais = AISCloner.clone(ais, new ProtobufWriter.SingleSchemaSelector(schema));
            Space curSpace = AisToSpace.create(ais);
            Space newSpace = Space.create(new InputStreamReader(postInput));
            SpaceDiff diff = new SpaceDiff(curSpace, newSpace);

            boolean success = true;
            JsonDiffPreview jsonSummary = new JsonDiffPreview();
            if(doApply) {
                DDLBasedSpaceModifier modifier = new DDLBasedSpaceModifier(reqs.dxlService.ddlFunctions(), session, schema, newSpace);
                diff.apply(modifier);
                if(modifier.hadError()) {
                    success = false;
                    for(String err : modifier.getErrors()) {
                        jsonSummary.error(err);
                    }
                }
            }
            if(success) {
                diff.apply(jsonSummary);
            }

            String json = jsonSummary.getJSON();
            return Response.status(Response.Status.OK).entity(json).build();
        } catch (Exception e) {
            // TODO: Cleanup and make consistent with other REST
            // While errors are still common, make them obvious.
            throw new WebApplicationException(
                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity(e.getMessage())
                            .build()
            );
        }
    }

    private String getUserSchema(HttpServletRequest request) {
        Principal user = request.getUserPrincipal();
        return (user == null) ? null : user.getName();
    }
}
