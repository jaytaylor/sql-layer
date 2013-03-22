
package com.akiban.ais.model.validation;

import java.util.HashSet;
import java.util.Set;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.JoinToMultipleParentsException;


/**
 * Validates each table has either zero or one join to a parent.  
 * @author tjoneslo
 *
 */
public class JoinToOneParent implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        Set<UserTable> childTables = new HashSet<>();
        
        for (Join join : ais.getJoins().values()) {
            if (childTables.contains(join.getChild())) {
                output.reportFailure(new AISValidationFailure (
                        new JoinToMultipleParentsException (join.getChild().getName())));
            } else {
                childTables.add(join.getChild());
            }
        }
    }

}
