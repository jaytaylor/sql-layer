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

package com.akiban.server.service.restdml;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.externaldata.ExternalDataService;
import com.akiban.server.service.security.SecurityService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;
import com.akiban.server.t3expressions.T3RegistryService;
import com.google.inject.Inject;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;

import static com.akiban.server.service.transaction.TransactionService.CloseableTransaction;

public class RestDMLServiceImpl implements Service, RestDMLService {

    private final SessionService sessionService;
    private final DXLService dxlService;
    private final TransactionService transactionService;
    private final SecurityService securityService;
    private final ExternalDataService extDataService;
    private final InsertProcessor insertProcessor;
    private final DeleteProcessor deleteProcessor;
    
    @Inject
    public RestDMLServiceImpl(SessionService sessionService,
                              DXLService dxlService,
                              TransactionService transactionService,
                              SecurityService securityService,
                              ExternalDataService extDataService,
                              ConfigurationService configService,
                              TreeService treeService,
                              Store store,
                              T3RegistryService registryService) {
        this.sessionService = sessionService;
        this.dxlService = dxlService;
        this.transactionService = transactionService;
        this.securityService = securityService;
        this.extDataService = extDataService;
        this.insertProcessor = new InsertProcessor (configService, treeService, store, registryService);
        this.deleteProcessor = new DeleteProcessor (configService, treeService, store, registryService);
    }
    
    /* Service */

    @Override
    public void start() {
        // None
    }

    @Override
    public void stop() {
        // None
   }

    @Override
    public void crash() {
        //None
    }
    
    /* RestDMLService */

    @Override
    public Response getAllEntities(final HttpServletRequest request, final String schema, final String table, Integer depth) {
        if (!securityService.isAccessible(request, schema))
            return Response.status(Response.Status.FORBIDDEN).build();
        final int realDepth = (depth != null) ? Math.max(depth, 0) : -1;
        return Response.status(Response.Status.OK)
                .entity(new StreamingOutput() {
                    @Override
                    public void write(OutputStream output) throws IOException {
                        try (Session session = sessionService.createSession()) {
                            // Do not auto-close writer as that prevents an exception from propagating to the client
                            PrintWriter writer = new PrintWriter(output);
                            extDataService.dumpAllAsJson(session, writer, schema, table, realDepth, true);
                            writer.write('\n');
                            writer.close();
                        } catch(InvalidOperationException e) {
                            throwToClient(e);
                        }
                    }
                })
                .build();
    }

    @Override
    public Response getEntities(final HttpServletRequest request, final String schema, final String table, Integer inDepth, final String identifiers) {
        if (!securityService.isAccessible(request, schema))
            return Response.status(Response.Status.FORBIDDEN).build();
        final TableName tableName = new TableName(schema, table);
        final int depth = (inDepth != null) ? Math.max(inDepth, 0) : -1;
        return Response.status(Response.Status.OK)
                .entity(new StreamingOutput() {
                    @Override
                    public void write(OutputStream output) throws IOException {
                        try (Session session = sessionService.createSession();
                             CloseableTransaction txn = transactionService.beginCloseableTransaction(session)) {
                            // Do not auto-close writer as that prevents an exception from propagating to the client
                            PrintWriter writer = new PrintWriter(output);
                            UserTable uTable = dxlService.ddlFunctions().getUserTable(session, tableName);
                            Index pkIndex = uTable.getPrimaryKeyIncludingInternal().getIndex();
                            List<List<String>> pks = PrimaryKeyParser.parsePrimaryKeys(identifiers, pkIndex);
                            extDataService.dumpBranchAsJson(session, writer, schema, table, pks, depth, false);
                            writer.write('\n');
                            txn.commit();
                            writer.close();
                        } catch(InvalidOperationException e) {
                            throwToClient(e);
                        }
                    }
                })
                .build();
    }

    @Override
    public Response insert(final HttpServletRequest request, final String schemaName, final String tableName, JsonNode node)  {
        if (!securityService.isAccessible(request, schemaName))
            return Response.status(Response.Status.FORBIDDEN).build();
        TableName rootTable = new TableName (schemaName, tableName);
        try (Session session = sessionService.createSession();
            CloseableTransaction txn = transactionService.beginCloseableTransaction(session)) {
            AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(session);
            String pk = insertProcessor.processInsert(session, ais, rootTable, node);
            txn.commit();
            return Response.status(Response.Status.OK)
                .entity(pk).build();
        } catch (JsonParseException ex) {
            throw new WebApplicationException(
                    Response.status(Response.Status.BAD_REQUEST)
                            .entity(ex.toString())
                            .build());
        } catch (IOException e) {
            throw new WebApplicationException(
                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(e.toString())
                        .build());
        } catch (InvalidOperationException e) {
            throwToClient(e);
        }
        assert false : "No value returned from insert";
        return null;
    }

    @Override
    public Response delete(final HttpServletRequest request, final String schema, final String table, final String identifier) {
        if (!securityService.isAccessible(request, schema))
            return Response.status(Response.Status.FORBIDDEN).build();
        final TableName tableName = new TableName (schema, table);
        
        try (Session session = sessionService.createSession();
                CloseableTransaction txn = transactionService.beginCloseableTransaction(session)) {
            AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(session);
            deleteProcessor.processDelete(session, ais, tableName, identifier);
            txn.commit();
            return Response.status(Response.Status.OK)
                    .entity("")
                    .build();
        } catch (InvalidOperationException e) {
            throwToClient(e);
        }
        assert false : "No value returned from insert";
        return null;
    }

    private void throwToClient(InvalidOperationException e) {
        StringBuilder err = new StringBuilder(100);
        err.append("[{\"code\":\"");
        err.append(e.getCode().getFormattedValue());
        err.append("\",\"message\":\"");
        err.append(e.getMessage());
        err.append("\"}]\n");
        // TODO: Map various IOEs to other codes?
        final Response.Status status;
        if(e instanceof NoSuchTableException) {
            status = Response.Status.NOT_FOUND;
         } else {
            status = Response.Status.CONFLICT;
        }
        throw new WebApplicationException(
                Response.status(status)
                        .entity(err.toString())
                        .build()
        );
    }
}
