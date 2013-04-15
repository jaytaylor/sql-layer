/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.rest.resources;

import com.akiban.ais.model.TableName;
import com.akiban.http.SimpleHandlerList;
import com.akiban.rest.ResourceRequirements;
import com.akiban.rest.RestResponseBuilder;
import com.akiban.util.tap.InOutTap;
import com.fasterxml.jackson.databind.JsonNode;

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

import static com.akiban.rest.resources.ResourceHelper.IDENTIFIERS_MULTI;
import static com.akiban.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;
import static com.akiban.rest.resources.ResourceHelper.checkTableAccessible;
import static com.akiban.rest.resources.ResourceHelper.getPKString;
import static com.akiban.rest.resources.ResourceHelper.parseTableName;
import static com.akiban.util.JsonUtils.readTree;

/**
 * Entity based access (GET), creation (PUT, POST), and modification (PUT, DELETE)
 */
@Path("/entity/{entity}")
public class EntityResource {
    private final ResourceRequirements reqs;
    private static final InOutTap ENTITY_GET = SimpleHandlerList.REST_TAP.createSubsidiaryTap("rest: entity GET");
    private static final InOutTap ENTITY_POST = SimpleHandlerList.REST_TAP.createSubsidiaryTap("rest: entity POST");
    private static final InOutTap ENTITY_PUT = SimpleHandlerList.REST_TAP.createSubsidiaryTap("rest: entity PUT");
    private static final InOutTap ENTITY_DELETE = SimpleHandlerList.REST_TAP.createSubsidiaryTap("rest: entity DELETE");
    private static final InOutTap ENTITY_PATCH = SimpleHandlerList.REST_TAP.createSubsidiaryTap("rest: entity PATCH");
    private static final InOutTap ENTITY_JONQUIL = SimpleHandlerList.REST_TAP.createSubsidiaryTap("rest: entity jonquil");
    
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
        ENTITY_GET.in();
        try {
            return RestResponseBuilder
                    .forRequest(request)
                    .body(new RestResponseBuilder.BodyGenerator() {
                        @Override
                        public void write(PrintWriter writer) throws Exception {
                            reqs.restDMLService.getAllEntities(writer, tableName, depth);
                        }
                    })
                    .build();
        } finally {
            ENTITY_GET.out();
        }
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
        ENTITY_GET.in();
        try {
            return RestResponseBuilder
                    .forRequest(request)
                    .body(new RestResponseBuilder.BodyGenerator() {
                        @Override
                        public void write(PrintWriter writer) throws Exception {
                            reqs.restDMLService.getEntities(writer, tableName, depth, getPKString(uri));
                        }
                    })
                    .build();
        } finally {
            ENTITY_GET.out();
        }
    }

    @POST
    @Path("/jonquil/to-sql")
    @Consumes(MEDIATYPE_JSON_JAVASCRIPT)
    public Response jonquilToSQL(@Context final HttpServletRequest request,
                                 @PathParam("entity") String entity,
                                 final String query) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        writer.write(reqs.restDMLService.jonquilToSQL(tableName, query));
                    }
                })
                .build();
    }

    @POST
    @Path("/jonquil/query")
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response jonquilQuery(@Context final HttpServletRequest request,
                                 @PathParam("entity") String entity,
                                 final String query) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        ENTITY_JONQUIL.in();
        try {
            return RestResponseBuilder
                    .forRequest(request)
                    .body(new RestResponseBuilder.BodyGenerator() {
                        @Override
                        public void write(PrintWriter writer) throws Exception {
                            String sql = reqs.restDMLService.jonquilToSQL(tableName, query);
                            reqs.restDMLService.runSQL(writer, request, sql, tableName.getSchemaName());
                        }
                    })
                    .build();
        } finally {
            ENTITY_JONQUIL.out();
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response createEntity(@Context HttpServletRequest request,
                                 @PathParam("entity") String entity,
                                 final byte[] entityBytes) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        ENTITY_POST.in();
        try {
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
        } finally {
            ENTITY_POST.out();
        }
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
        ENTITY_PUT.in();
        try {
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
        } finally {
            ENTITY_PUT.out();
        }
    }

    @DELETE
    @Path("/" + IDENTIFIERS_MULTI)
    public Response deleteEntity(@Context HttpServletRequest request,
                                 @PathParam("entity") String entity,
                                 @Context final UriInfo uri) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        ENTITY_DELETE.in();
        try {
            reqs.restDMLService.delete(tableName, getPKString(uri));
            return RestResponseBuilder
                    .forRequest(request)
                    .status(Response.Status.NO_CONTENT)
                    .build();
        } catch (Exception e) {
            throw RestResponseBuilder.forRequest(request).wrapException(e);
        } finally {
            ENTITY_DELETE.out();
        }
    }
    
    @PATCH
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response patchEntity(@Context HttpServletRequest request,
            @PathParam("entity") String entity,
            final byte[] entityBytes) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        ENTITY_PATCH.in();
        try {
            return RestResponseBuilder
                    .forRequest(request)
                    .body(new RestResponseBuilder.BodyGenerator() {
                        @Override
                        public void write(PrintWriter writer) throws Exception {
                            final JsonNode node = readTree(entityBytes);
                            reqs.restDMLService.upsert(writer, tableName, node);
                        }
                    })
                    .build();
        } finally {
            ENTITY_PATCH.out();
        }
    }
}
