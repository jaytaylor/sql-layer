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

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import org.codehaus.jackson.JsonParser;

import com.akiban.ais.model.TableName;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.akiban.server.t3expressions.T3RegistryService;
import com.akiban.sql.optimizer.plan.PhysicalUpdate;
import com.google.inject.Inject;

public class RestDMLServiceImpl implements Service, RestDMLService {

    private ConfigurationService configService;
    private SchemaManager schemaManager;
    private SessionService sessionService;
    
    private DXLService dxlService;
    private Store store;
    private TransactionService transactionService;
    private TreeService treeService;
    private T3RegistryService t3RegistryService;
    private OperatorCache operatorCache;
    
    @Inject
    public RestDMLServiceImpl (ConfigurationService configService,
            SessionService sessionService,
            SchemaManager schemaService,
            T3RegistryService registryService,
            
            DXLService dxlService, Store store,
            TransactionService transactionService,
            TreeService treeService) {
        this.configService = configService;
        this.schemaManager = schemaService;
        this.sessionService = sessionService;
        this.t3RegistryService = registryService;
        
        this.dxlService = dxlService;
        this.store = store;
        this.transactionService = transactionService;
        this.treeService = treeService;
        this.operatorCache = new OperatorCache (schemaManager, t3RegistryService);
        
    }
    
    /* service */
    @Override
    public void start() {
        //None
    }

    @Override
    public void stop() {
        //None
    }

    @Override
    public void crash() {
        //None
    }
    
    /* RestDML Service Impl */
    public Response insert(final String schemaName, final String tableName, JsonParser jp)  {
        TableName rootTable = new TableName (schemaName, tableName);
        try {
            Session session = sessionService.createSession();
            PhysicalUpdate update = operatorCache.getInsertOperator(session, rootTable);
            
            String pk = "";
            
            return Response.status(Response.Status.OK)
                .entity(pk).build();
        } catch (InvalidOperationException e) {
            throwToClient(e);
        }
    }

    private void throwToClient(InvalidOperationException e) {
        StringBuilder err = new StringBuilder(100);
        err.append("[{\"code\":\"");
        err.append(e.getCode().getFormattedValue());
        err.append("\",\"message\":\"");
        err.append(e.getMessage());
        err.append("\"}]\n");
        throw new WebApplicationException(
                Response.status(Response.Status.NOT_FOUND)
                        .entity(err.toString())
                        .build()
        );
    }

}
