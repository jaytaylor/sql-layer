package com.akiban.cserver.itests;

import com.akiban.ais.model.*;
import com.akiban.ais.util.DDLGenerator;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.TableId;
import org.junit.Test;

public final class IndexesTest extends ApiTestBase {
    @Test
    public void createSingleIndex() throws InvalidOperationException {
        TableId tbl_id = createTable("test", "tbl", "id int primary key, name varchar(255)");
        AkibaInformationSchema full_ais = ddl().getAIS();
        
        AkibaInformationSchema index_ais = new AkibaInformationSchema();
        UserTable index_tbl = UserTable.create(index_ais, "test", "tbl", ddl().resolveTableId(tbl_id).getTableId(null));
        Column index_tcol = Column.create(index_tbl, "name", 1, index_ais.getType("varchar"));
        index_tcol.setTypeParameter1(255L);
        Index index_idx = Index.create(index_ais, index_tbl, "name", -1, false, "INDEX");
        index_idx.addColumn(new IndexColumn(index_idx, index_tcol, 0, true, 255));

        DDLGenerator gen = new DDLGenerator();
        System.out.println(gen.createTable(full_ais.getUserTable("test", "tbl")));
        ddl().createIndexes(index_ais);
        System.out.println(gen.createTable(full_ais.getUserTable("test", "tbl")));
    }
}
