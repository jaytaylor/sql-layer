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

package com.akiban.server.service.routines;

import com.akiban.ais.model.Routine;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.ais.model.aisb2.NewRoutineBuilder;
import com.akiban.server.service.Service;
import com.akiban.server.store.AisHolder;
import com.akiban.server.store.SchemaManager;

import com.akiban.qp.loadableplan.std.DumpGroupLoadablePlan;
import com.akiban.qp.loadableplan.std.PersistitCLILoadablePlan;

import javax.inject.Inject;
import com.google.inject.Singleton;

import java.util.Collection;

@Singleton
public final class RoutineLoaderImpl implements RoutineLoader, Service {

    private AisHolder aisHolder;
    private SchemaManager schemaManager;

    @Inject @SuppressWarnings("unused")
    public RoutineLoaderImpl(AisHolder aisHolder, 
                             SchemaManager schemaManager) {
        this.aisHolder = aisHolder;
        this.schemaManager = schemaManager;
    }

    /* Service */

    @Override
    public void start() {
        registerSystemProcedures();
    }

    @Override
    public void stop() {
        if (false)              // Only started once for server and AIS wiped for tests.
            unregisterSystemProcedures();
    }

    @Override
    public void crash() {
        stop();
    }

    public static final int IDENT_MAX = 128;
    public static final int COMMAND_MAX = 1024;

    private void registerSystemProcedures() {
        NewAISBuilder aisb = AISBBasedBuilder.create();

        aisb.defaultSchema(TableName.SYS_SCHEMA);
        aisb.procedure("dump_group")
            .paramStringIn("SCHEMA_NAME", IDENT_MAX)
            .paramStringIn("TABLE_NAME", IDENT_MAX)
            .paramLongIn("INSERT_MAX_ROW_COUNT")
            .externalName("com.akiban.qp.loadableplan.std.DumpGroupLoadablePlan");
        aisb.procedure("persistitcli")
            .paramStringIn("COMMAND", COMMAND_MAX)
            .externalName("com.akiban.qp.loadableplan.std.PersistitCLILoadablePlan");

        Collection<Routine> procs = aisb.ais().getRoutines().values();
        for (Routine proc : procs) {
            schemaManager.registerSystemRoutine(proc);
        }
    }

    private void unregisterSystemProcedures() {
        schemaManager.unRegisterSystemRoutine(new TableName(TableName.SYS_SCHEMA,
                                                            "dump_group"));
        schemaManager.unRegisterSystemRoutine(new TableName(TableName.SYS_SCHEMA,
                                                            "persistitcli"));
    }
}
