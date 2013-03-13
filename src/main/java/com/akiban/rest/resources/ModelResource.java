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
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.protobuf.ProtobufWriter;
import com.akiban.ais.util.UuidAssigner;
import com.akiban.rest.ResourceRequirements;
import com.akiban.rest.RestResponseBuilder;
import com.akiban.server.entity.changes.DDLBasedSpaceModifier;
import com.akiban.server.entity.changes.EntityParser;
import com.akiban.server.entity.changes.SpaceDiff;
import com.akiban.server.entity.fromais.AisToSpace;
import com.akiban.server.entity.model.Space;
import com.akiban.server.entity.model.diff.JsonDiffPreview;
import com.akiban.server.service.session.Session;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
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

import static com.akiban.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;
import static com.akiban.rest.resources.ResourceHelper.checkSchemaAccessible;
import static com.akiban.rest.resources.ResourceHelper.checkTableAccessible;
import static com.akiban.server.service.transaction.TransactionService.CloseableTransaction;

@Path("/model")
public final class ModelResource {
    private static final String OPTIONAL_SCHEMA = "{schema: (/[^/]*)?}";

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
                        try (Session session = reqs.sessionService.createSession();
                             CloseableTransaction txn = reqs.transactionService.beginCloseableTransaction(session)) {
                            Space space = spaceForAIS(session, schema);
                            String json = space.toJson();
                            writer.write(json);
                            txn.commit();
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
                          final InputStream postInput) {
        final TableName tableName = ResourceHelper.parseTableName(request, table);
        checkTableAccessible(reqs.securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        boolean doCreate = Boolean.parseBoolean(create);
                        ObjectMapper m = new ObjectMapper();
                        JsonNode node = m.readTree(postInput);
                        EntityParser parser = new EntityParser();
                        try (Session session = reqs.sessionService.createSession()) {
                            final UserTable created;
                            if(doCreate) {
                                created = parser.parseAndCreate(reqs.dxlService.ddlFunctions(),
                                                                session,
                                                                tableName,
                                                                node);
                            } else {
                                created = parser.parse(tableName, node);
                                UuidAssigner uuidAssigner = new UuidAssigner();
                                created.getAIS().traversePostOrder(uuidAssigner);
                                LOG.error("Table {}", created.toString());
                            }
                            Space currSpace = spaceForAIS(created.getAIS(), tableName.getSchemaName());
                            writer.write(currSpace.toJson());
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
                            SpaceDiff diff = new SpaceDiff(curSpace, newSpace);

                            boolean success = true;
                            JsonDiffPreview jsonSummary = new JsonDiffPreview(writer);
                            if(doApply) {
                                DDLBasedSpaceModifier modifier = new DDLBasedSpaceModifier(reqs.dxlService.ddlFunctions(), session, schema, newSpace);
                                diff.apply(modifier);
                                if(modifier.hadError()) {
                                    success = false;
                                    for(String err : modifier.getErrors()) {
                                        jsonSummary.error(err);
                                    }
                                }
                                // re-create the diff against the new AIS
                                diff = new SpaceDiff(curSpace, spaceForAIS(session, schema));
                            }
                            if(success) {
                                diff.apply(jsonSummary);
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
        ais = AISCloner.clone(ais, new ProtobufWriter.SingleSchemaSelector(schema));
        return AisToSpace.create(ais, Space.requireUUIDs);
    }
    private static final Logger LOG = LoggerFactory.getLogger(ModelResource.class);    
}
