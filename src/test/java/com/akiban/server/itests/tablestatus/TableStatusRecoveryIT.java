package com.akiban.server.itests.tablestatus;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;

import com.akiban.server.TableStatistics;
import com.akiban.server.itests.ApiTestBase;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.config.Property;
import com.persistit.Persistit;

public class TableStatusRecoveryIT extends ApiTestBase {
    
    @Test
    public void insertRowCountTest() throws Exception {
        int tableId = createTable("test", "A", "I INT, V VARCHAR(255), PRIMARY KEY(I)");
        for (int i = 0; i < 10000; i++) {
            writeRows(createNewRow(tableId, i, "This is record # " + 1));
        }
        final TableStatistics ts1 = store().getTableStatistics(session(), tableId);
        assertEquals(10000, ts1.getRowCount());
        
        final Persistit db = serviceManager().getTreeService().getDb();

        final String datapath = db.getProperty("datapath");
        db.getJournalManager().force();
        crashTestServices();
      
        final Property property = new Property(Property.parseKey("akserver.datapath"), datapath);
        final Collection<Property> extraProperties = Arrays.asList(new Property[]{property});
        restartTestServices(extraProperties);
        
        final TableStatistics ts2 = store().getTableStatistics(session(), tableId);
        assertEquals(10000, ts2.getRowCount());

    }

}
