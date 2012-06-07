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

package com.akiban.server.test.it.store;

import com.akiban.server.store.PersistitStoreSchemaManager;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.test.it.ITBase;
import com.google.inject.ProvisionException;
import org.junit.Before;
import org.junit.Test;

import static com.akiban.server.test.it.store.SchemaManagerIT.*;
import static com.akiban.server.store.PersistitStoreSchemaManager.SerializationType;
import static org.junit.Assert.assertEquals;

public class PersistitStoreSchemaManagerIT extends ITBase {
    private PersistitStoreSchemaManager pssm;

    private PersistitStoreSchemaManager castToPSSM() {
        SchemaManager schemaManager = serviceManager().getSchemaManager();
        if(schemaManager instanceof PersistitStoreSchemaManager) {
            return (PersistitStoreSchemaManager)schemaManager;
        } else {
            throw new IllegalStateException("Expected PersistitStoreSchemaManager!");
        }
    }

    @Before
    public void setUpPSSM() {
        pssm = castToPSSM();
    }

    private void safeRestart() throws Exception {
        safeRestartTestServices();
        pssm = castToPSSM();
    }

    @Test
    public void existingMetaModelReadAndUpgraded() throws Exception {
        pssm.setSerializationType(SerializationType.META_MODEL);
        createTable(SCHEMA, T1_NAME, T1_DDL);

        safeRestart();

        assertEquals("Protobuf after load", SerializationType.PROTOBUF, pssm.getSerializationType());
        createTable(SCHEMA, T2_NAME, T2_DDL);
        assertEquals("Still Protobuf after save", "[PROTOBUF]", pssm.getAllSerializationTypes(session()).toString());
    }

    @Test
    public void newDataSetReadAndSavedAsProtobuf() throws Exception {
        assertEquals("No type on new volume", SerializationType.NONE, pssm.getSerializationType());
        createTable(SCHEMA, T1_NAME, T1_DDL);
        assertEquals("Saved as PROTOBUF", SerializationType.PROTOBUF, pssm.getSerializationType());

        safeRestart();

        assertEquals("Saw PROTOBUF on load", SerializationType.PROTOBUF, pssm.getSerializationType());
    }

    // Provision = error during startup of PSSM
    @Test(expected=ProvisionException.class)
    public void mixedMetaModelAndProtobufIsIllegal() throws Exception {
        castToPSSM();

        // Create a bad volume on purpose to make sure we detect on load
        pssm.setSerializationType(SerializationType.META_MODEL);
        createTable(SCHEMA, T1_NAME, T1_DDL);

        pssm.setSerializationType(SerializationType.PROTOBUF);
        createTable(SCHEMA, T2_NAME, T2_DDL);

        safeRestart();
    }
}
