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
import com.akiban.rest.ResourceRequirements;
import com.akiban.rest.RestResponseBuilder;
import com.akiban.server.service.restdml.ModelBuilder;
import com.akiban.server.service.restdml.RestDMLService;
import com.akiban.server.service.security.SecurityService;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.PrintWriter;

import static com.akiban.rest.resources.ResourceHelper.IDENTIFIERS_MULTI;
import static com.akiban.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;
import static com.akiban.rest.resources.ResourceHelper.checkTableAccessible;
import static com.akiban.rest.resources.ResourceHelper.getPKString;
import static com.akiban.rest.resources.ResourceHelper.parseTableName;

@Path("/builder")
public class BuilderResource {
    private final SecurityService securityService;
    private final RestDMLService restDMLService;
    private final ModelBuilder modelBuilder;

    public BuilderResource(ResourceRequirements reqs) {
        this.securityService = reqs.securityService;
        this.restDMLService = reqs.restDMLService;
        this.modelBuilder = new ModelBuilder(
                reqs.sessionService,
                reqs.transactionService,
                reqs.dxlService,
                reqs.store,
                reqs.treeService,
                reqs.configService,
                restDMLService
        );
    }

    @GET
    @Path("/entity/{entity}")
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response getAll(@Context HttpServletRequest request,
                           @PathParam("entity") String entityName) {
        final TableName tableName = parseTableName(request, entityName);
        checkTableAccessible(securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        modelBuilder.create(tableName);
                        restDMLService.getAllEntities(writer, tableName, 0);
                    }
                })
                .build();
    }

    @GET
    @Path("/entity/{entity}/" + IDENTIFIERS_MULTI)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response getSingle(@Context HttpServletRequest request,
                              @PathParam("entity") String entityName,
                              @Context final UriInfo uri) {
        final TableName tableName = parseTableName(request, entityName);
        checkTableAccessible(securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        modelBuilder.create(tableName);
                        restDMLService.getEntities(writer, tableName, 0, getPKString(uri));
                    }
                })
                .build();
    }

    @POST
    @Path("/entity/{entity}")
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response post(@Context HttpServletRequest request,
                         @PathParam("entity") String entityName,
                         final String entityData) {
        final TableName tableName = parseTableName(request, entityName);
        checkTableAccessible(securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        modelBuilder.insert(writer, tableName, entityData);
                    }
                })
                .build();
    }

    @PUT
    @Path("/entity/{entity}/" + IDENTIFIERS_MULTI)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response put(@Context HttpServletRequest request,
                        @PathParam("entity") String entityName,
                        @Context final UriInfo uri,
                        final String entityData) {
        final TableName tableName = parseTableName(request, entityName);
        checkTableAccessible(securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        modelBuilder.update(writer, tableName, getPKString(uri), entityData);
                    }
                })
                .build();
    }

    @DELETE
    @Path("/entity/{entity}/" + IDENTIFIERS_MULTI)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response delete(@Context HttpServletRequest request,
                           @PathParam("entity") String entityName,
                           @Context final UriInfo uri) {
        final TableName tableName = parseTableName(request, entityName);
        checkTableAccessible(securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        modelBuilder.create(tableName);
                        restDMLService.delete(writer, tableName, getPKString(uri));
                    }
                })
                .build();
    }

    @POST
    @Path("/explode/{entity}")
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response explode(@Context HttpServletRequest request,
                            @PathParam("entity") String entityName) {
        final TableName tableName = parseTableName(request, entityName);
        checkTableAccessible(securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        modelBuilder.explode(writer, tableName);
                    }
                })
                .build();
    }

    @POST
    @Path("/implode/{entity}")
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response implode(@Context HttpServletRequest request,
                            @PathParam("entity") String entityName) {
        final TableName tableName = parseTableName(request, entityName);
        checkTableAccessible(securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        modelBuilder.implode(writer, tableName);
                    }
                })
                .build();
    }
}
