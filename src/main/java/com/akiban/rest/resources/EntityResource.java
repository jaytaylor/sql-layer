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
import com.akiban.rest.ResourceRequirements;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.akiban.rest.RestResponseBuilder;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.PrintWriter;

import static com.akiban.rest.resources.ResourceHelper.parseTableName;
import static com.akiban.rest.resources.ResourceHelper.checkTableAccessible;

/**
 * Entity based access (GET), creation (PUT, POST), and modification (PUT, DELETE)
 */
@Path("/entity/{entity}")
public class EntityResource {
    private static final String IDENTIFIERS_MULTI = "{identifiers:.*}";
    private final ResourceRequirements reqs;
    
    public EntityResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response retrieveEntity(@Context HttpServletRequest request,
                                   @QueryParam("jsonp") String jsonp,
                                   @PathParam("entity") String entity,
                                   @QueryParam("depth") final Integer depth) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        return RestResponseBuilder
                .forJsonp(jsonp)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.getAllEntities(writer, tableName, depth);
                    }
                })
                .build();
    }

    @GET
    @Path("/" + IDENTIFIERS_MULTI)
    @Produces(MediaType.APPLICATION_JSON)
    public Response retrieveEntity(@Context HttpServletRequest request,
                                   @QueryParam("jsonp") String jsonp,
                                   @PathParam("entity") String entity,
                                   @QueryParam("depth") final Integer depth,
                                   @Context final UriInfo uri) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        return RestResponseBuilder
                .forJsonp(jsonp)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.getEntities(writer, tableName, depth, getPKString(uri));
                    }
                })
                .build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createEntity(@Context HttpServletRequest request,
                                 @QueryParam("jsonp") String jsonp,
                                 @PathParam("entity") String entity,
                                 final byte[] entityBytes) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        return RestResponseBuilder
                .forJsonp(jsonp)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        ObjectMapper m = new ObjectMapper();
                        final JsonNode node = m.readTree(entityBytes);
                        reqs.restDMLService.insert(writer, tableName, node);
                    }
                })
                .build();
    }

    @PUT
    @Path("/" + IDENTIFIERS_MULTI)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateEntity(@Context HttpServletRequest request,
                                 @QueryParam("jsonp") final String jsonp,
                                 @PathParam("entity") String entity,
                                 final byte[] entityBytes,
                                 @Context final UriInfo uri) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        return RestResponseBuilder
                .forJsonp(jsonp)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        ObjectMapper m = new ObjectMapper();
                        JsonNode node = m.readTree(entityBytes);
                        reqs.restDMLService.update(writer, tableName, getPKString(uri), node);
                    }
                })
                .build();
    }

    @DELETE
    @Path("/" + IDENTIFIERS_MULTI)
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteEntity(@Context HttpServletRequest request,
                                 @QueryParam("jsonp") String jsonp,
                                 @PathParam("entity") String entity,
                                 @Context final UriInfo uri) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        return RestResponseBuilder
                .forJsonp(jsonp)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.delete(writer, tableName, getPKString(uri));
                    }
                })
                .build();
    }


    private static String getPKString(UriInfo uri) {
        String pks[] = uri.getPath(false).split("/");
        assert pks.length > 0: uri;
        return pks[pks.length - 1];
    }
}
