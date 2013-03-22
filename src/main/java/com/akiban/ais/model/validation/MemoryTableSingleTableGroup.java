
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Join;
import com.akiban.server.error.GroupMultipleMemoryTables;

/**
 * Validate the current assumption of groups with a memory table contain only one 
 * table, that is, there is no muti-table groups with the memory tables. 
 * (The MemoryTablesNotMixed validation ensures Memory tables are not mixed with other 
 *  types of tables). 
 *  TODO: It would be nice to remove this limitation of the current system. 
 * @author tjoneslo
 *
 */
public class MemoryTableSingleTableGroup implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Join join : ais.getJoins().values()) {
            if (join.getChild().hasMemoryTableFactory()) {
                output.reportFailure(new AISValidationFailure (
                        new GroupMultipleMemoryTables(join.getParent().getName(), join.getChild().getName())));
            }
        }
    }

}
