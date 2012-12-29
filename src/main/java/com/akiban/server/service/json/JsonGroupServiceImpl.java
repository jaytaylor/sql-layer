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

package com.akiban.server.service.json;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.CacheValueGenerator;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.service.Service;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.transaction.TransactionService;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class JsonGroupServiceImpl implements JsonGroupService, Service {
    private DXLService dxlService;
    private TransactionService transactionService;
    
    private static final Logger logger = LoggerFactory.getLogger(JsonGroupServiceImpl.class);

    @Inject
    public JsonGroupServiceImpl(DXLService dxlService,
                                TransactionService transactionService) {
        this.dxlService = dxlService;
        this.transactionService = transactionService;
    }

    /* JsonGroupService */

    @Override
    public void writeGroup(Session session,
                           String schemaName, String tableName, List<String> keys,
                           OutputStream outputStream, String encoding) 
            throws IOException {
        AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(session);
        UserTable table = ais.getUserTable(schemaName, tableName);
        if (table == null)
            // TODO: Consider sending in-band as JSON.
            throw new NoSuchTableException(schemaName, tableName);
        GroupPlanGenerator generator = 
            ais.getCachedValue(this,
                               new CacheValueGenerator<GroupPlanGenerator>() {
                                   @Override
                                   public GroupPlanGenerator valueFor(AkibanInformationSchema ais) {
                                       return new GroupPlanGenerator(ais);
                                   }
                               });
        Operator plan = generator.generate(table);
        outputStream.write(table.getName().toString().getBytes(encoding));
    }

    /* Service */
    
    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void crash() {
    }

}
