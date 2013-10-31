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

import com.foundationdb.ais.AISCloner;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.protobuf.ProtobufWriter;
import com.foundationdb.ais.util.UuidAssigner;
import com.foundationdb.rest.ResourceRequirements;
import com.foundationdb.rest.RestResponseBuilder;
import com.foundationdb.server.entity.changes.DDLBasedSpaceModifier;
import com.foundationdb.server.entity.changes.EntityParser;
import com.foundationdb.server.entity.changes.SpaceDiff;
import com.foundationdb.server.entity.fromais.AisToSpace;
import com.foundationdb.server.entity.model.Space;
import com.foundationdb.server.entity.model.diff.JsonDiffPreview;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.Tap;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.security.Principal;

import static com.foundationdb.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;
import static com.foundationdb.rest.resources.ResourceHelper.checkSchemaAccessible;
import static com.foundationdb.rest.resources.ResourceHelper.checkTableAccessible;
import static com.foundationdb.server.service.transaction.TransactionService.CloseableTransaction;
import static com.foundationdb.util.JsonUtils.readTree;

@Path("/model")
public final class ModelResource {
    private static final String OPTIONAL_SCHEMA = "{schema: (/[^/]*)?}";
    private static final InOutTap MODEL_VIEW = Tap.createTimer("rest: model view");
    private static final InOutTap MODEL_HASH = Tap.createTimer("rest: model view");
    private static final InOutTap MODEL_PARSE = Tap.createTimer("rest: model parse");
    
    private final ResourceRequirements reqs;

    public ModelResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    private static String getSchemaName(HttpServletRequest request, String schemaParam) {
        if(schemaParam == null || schemaParam.length() <= 1) { // empty or just /
            Principal user = request.getUserPrincipal();
            return (user == null) ? null : user.getName();
        }
        return schemaParam.substring(1);
    }

    @GET
    @Path("/view" + OPTIONAL_SCHEMA)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response viewSpace(@Context HttpServletRequest request,
                              @PathParam("schema") String schemaParam) {
        final String schema = getSchemaName(request, schemaParam);
        checkSchemaAccessible(reqs.securityService, request, schema);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        MODEL_VIEW.in();
                        try (Session session = reqs.sessionService.createSession();
                             CloseableTransaction txn = reqs.transactionService.beginCloseableTransaction(session)) {
                            Space space = spaceForAIS(session, schema);
                            String json = space.toJson();
                            writer.write(json);
                            txn.commit();
                        } finally {
                            MODEL_VIEW.out();
                        }
                    }
                })
                .build();
    }
    
    @GET
    @Path("/hash" + OPTIONAL_SCHEMA)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response hashOfSpace(@Context HttpServletRequest request,
                                @PathParam("schema") String schemaParam) {
        final String schema = getSchemaName(request, schemaParam);
        checkSchemaAccessible(reqs.securityService, request, schema);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        MODEL_HASH.in();
                        try (Session session = reqs.sessionService.createSession();
                             CloseableTransaction txn = reqs.transactionService.beginCloseableTransaction(session)) {
                            Space space = spaceForAIS(session, schema);
                            String json = space.toHash();
                            writer.write(json);
                            txn.commit();
                        } finally {
                            MODEL_HASH.out();
                        }
                    }
                })
                .build();
    }

    @POST
    @Path("/parse/{table}")
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response parse(@Context HttpServletRequest request,
                          @PathParam("table") String table,
                          @QueryParam("create") final String create,
                          @QueryParam("defaultWidth") final String defaultWidth,
                          final InputStream postInput) {
        final TableName tableName = ResourceHelper.parseTableName(request, table);
        checkTableAccessible(reqs.securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        boolean doCreate = Boolean.parseBoolean(create);
                        JsonNode node = readTree(postInput);
                        EntityParser parser = new EntityParser();
                        parser.setStringWidth(parseInt(defaultWidth, DEFAULT_STRING_WIDTH));
                        MODEL_PARSE.in();
                        try (Session session = reqs.sessionService.createSession()) {
                            final Table created;
                            if(doCreate) {
                                created = parser.parseAndCreate(reqs.dxlService.ddlFunctions(),
                                                                session,
                                                                tableName,
                                                                node);
                            } else {
                                created = parser.parse(tableName, node);
                                UuidAssigner uuidAssigner = new UuidAssigner();
                                created.getAIS().visit(uuidAssigner);
                            }
                            Space currSpace = spaceForAIS(created.getAIS(), tableName.getSchemaName());
                            writer.write(currSpace.toJson());
                        } finally {
                            MODEL_PARSE.out();
                        }
                    }
                })
                .build();
    }

    @POST
    @Path("/preview" + OPTIONAL_SCHEMA)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response previewChange(@Context HttpServletRequest request,
                                  @PathParam("schema") String schemaParam,
                                  final InputStream postInput) {
        String schema = getSchemaName(request, schemaParam);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, schema);
        return previewOrApply(request, schema, postInput, false);
    }

    @POST
    @Path("/apply" + OPTIONAL_SCHEMA)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response applyChange(@Context HttpServletRequest request,
                                @PathParam("schema") String schemaParam,
                                final InputStream postInput) {
        String schema = getSchemaName(request, schemaParam);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, schema);
        return previewOrApply(request, schema, postInput, true);
    }

    private Response previewOrApply(HttpServletRequest request, final String schema, final InputStream postInput, final boolean doApply) {
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        try (Session session = reqs.sessionService.createSession()) {
                            // Cannot have transaction when attempting to perform DDL
                            Space curSpace = spaceForAIS(session, schema);
                            Space newSpace = Space.create(new InputStreamReader(postInput), Space.randomUUIDs);

                            boolean success = true;
                            JsonDiffPreview jsonSummary = new JsonDiffPreview(writer);
                            if(doApply) {
                                DDLBasedSpaceModifier modifier = new DDLBasedSpaceModifier(reqs.dxlService.ddlFunctions(), session, schema, newSpace);
                                SpaceDiff.apply(curSpace, newSpace, modifier);
                                if(modifier.hadError()) {
                                    success = false;
                                    for(String err : modifier.getErrors()) {
                                        jsonSummary.error(err);
                                    }
                                }
                                // re-create the diff against the new AIS
                                newSpace = spaceForAIS(session, schema);
                            }
                            if(success) {
                                SpaceDiff.apply(curSpace, newSpace, jsonSummary);
                            }
                            if (doApply)
                                jsonSummary.describeModifiedEntities();
                            jsonSummary.finish();
                        }
                    }
                })
                .build();
    }

    private Space spaceForAIS(Session session, String schema) {
        return spaceForAIS(reqs.dxlService.ddlFunctions().getAIS(session), schema);
    }

    private Space spaceForAIS(AkibanInformationSchema ais, String schema) {
        AISCloner aisCloner = reqs.dxlService.ddlFunctions().getAISCloner();
        ais = aisCloner.clone(ais, new ProtobufWriter.SingleSchemaSelector(schema));
        return AisToSpace.create(ais, Space.requireUUIDs);
    }

    private static final int DEFAULT_STRING_WIDTH = 128;
    private static  int parseInt(String number, int defaultValue) {
        try {
            return Integer.parseInt(number);
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }
    private static final Logger LOG = LoggerFactory.getLogger(ModelResource.class);    
}
