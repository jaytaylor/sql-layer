
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.server.error.JoinColumnTypesMismatchException;

/**
 * validate the columns used for the join in the parent (PK) and 
 * the child are join-able, according to the AIS.
 * @author tjoneslo
 *
 */
public class JoinColumnTypesMatch implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Join join : ais.getJoins().values()) {
            if (join.getParent().getPrimaryKey() == null) {
                //bug 931258: Attempting to join to a table without a explicit PK,
                // causes getJoinColumns to throw an JoinParentNoExplicitPK exception. 
                // This is explicitly validated in JoinToParentPK
                continue;
            }
            for (JoinColumn column : join.getJoinColumns()) {
                Column parentCol = column.getParent();
                Column childCol = column.getChild();
                if(!ais.canTypesBeJoined(parentCol.getType().name(), childCol.getType().name())) {
                    output.reportFailure(new AISValidationFailure (
                            new JoinColumnTypesMismatchException (parentCol.getTable().getName(), parentCol.getName(),
                                    childCol.getTable().getName(), childCol.getName())));
                }
            }
        }
    }
}
