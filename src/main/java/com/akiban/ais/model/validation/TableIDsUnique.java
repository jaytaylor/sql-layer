
package com.akiban.ais.model.validation;

import java.util.Map;
import java.util.TreeMap;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.DuplicateTableIdException;

class TableIDsUnique implements AISValidation {
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        final Map<Integer, Table> tableIDList= new TreeMap<>();
        for (UserTable table : ais.getUserTables().values()) {
            checkTableID (output, tableIDList, table);
        }
    }
    
    private void checkTableID (AISValidationOutput failures, Map<Integer, Table> tableIDList, Table table) {
        if (tableIDList.containsKey(table.getTableId())) {
            TableName name = tableIDList.get(table.getTableId()).getName();
            
            failures.reportFailure(new AISValidationFailure (
                    new DuplicateTableIdException(table.getName(), name)));
        } else {
            tableIDList.put(table.getTableId(), table);
        }
    }
}
