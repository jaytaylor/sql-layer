package com.akiban.server.service.restdml;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.Operator;
import com.akiban.server.service.restdml.OperatorGenerator.RowStream;

public class UpdateGenerator extends OperatorGenerator {

    private UserTable table;

    public UpdateGenerator(AkibanInformationSchema ais) {
        super(ais);
    }

    @Override
    protected Operator create(TableName tableName) {
        table = ais().getUserTable(tableName);

        RowStream stream = assembleValueScan (table);
        stream = assembleProjectTable (stream, table);
        
        return null;
    }

}
