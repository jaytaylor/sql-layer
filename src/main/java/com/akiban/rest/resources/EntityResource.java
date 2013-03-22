
package com.akiban.rest.resources;

import com.akiban.ais.model.TableName;
import com.akiban.rest.ResourceRequirements;
import com.akiban.rest.RestResponseBuilder;
import org.codehaus.jackson.JsonNode;

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
import java.io.PrintWriter;

import static com.akiban.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;
import static com.akiban.rest.resources.ResourceHelper.checkTableAccessible;
import static com.akiban.rest.resources.ResourceHelper.parseTableName;
import static com.akiban.util.JsonUtils.readTree;

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
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response retrieveEntity(@Context HttpServletRequest request,
                                   @PathParam("entity") String entity,
                                   @QueryParam("depth") final Integer depth) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
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
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response retrieveEntity(@Context HttpServletRequest request,
                                   @PathParam("entity") String entity,
                                   @QueryParam("depth") final Integer depth,
                                   @Context final UriInfo uri) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.getEntities(writer, tableName, depth, getPKString(uri));
                    }
                })
                .build();
    }

    @POST
    @Path("/ajdax/to-sql")
    @Consumes(MEDIATYPE_JSON_JAVASCRIPT)
    public Response ajdaxToSQL(@Context final HttpServletRequest request,
                          @PathParam("entity") String entity,
                          final String query) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        writer.write(reqs.restDMLService.ajdaxToSQL(tableName, query));
                    }
                })
                .build();
    }

    @POST
    @Path("/ajdax/query")
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response ajdaxQuery(@Context final HttpServletRequest request,
                          @PathParam("entity") String entity,
                          final String query) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        String sql = reqs.restDMLService.ajdaxToSQL(tableName, query);
                        reqs.restDMLService.runSQL(writer, request, sql, tableName.getSchemaName());
                    }
                })
                .build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response createEntity(@Context HttpServletRequest request,
                                 @PathParam("entity") String entity,
                                 final byte[] entityBytes) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        final JsonNode node = readTree(entityBytes);
                        reqs.restDMLService.insert(writer, tableName, node);
                    }
                })
                .build();
    }

    @PUT
    @Path("/" + IDENTIFIERS_MULTI)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response updateEntity(@Context HttpServletRequest request,
                                 @PathParam("entity") String entity,
                                 final byte[] entityBytes,
                                 @Context final UriInfo uri) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        JsonNode node = readTree(entityBytes);
                        reqs.restDMLService.update(writer, tableName, getPKString(uri), node);
                    }
                })
                .build();
    }

    @DELETE
    @Path("/" + IDENTIFIERS_MULTI)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response deleteEntity(@Context HttpServletRequest request,
                                 @PathParam("entity") String entity,
                                 @Context final UriInfo uri) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
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
