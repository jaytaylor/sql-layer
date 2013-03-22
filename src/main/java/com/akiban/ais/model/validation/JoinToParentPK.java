
package com.akiban.ais.model.validation;

import java.util.Iterator;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.TableIndex;
import com.akiban.server.error.JoinColumnMismatchException;
import com.akiban.server.error.JoinParentNoExplicitPK;
import com.akiban.server.error.JoinToWrongColumnsException;

class JoinToParentPK implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Join join : ais.getJoins().values()) {
            
            // bug 931258: If parent has no external PK, flag this as an error. 
            if (join.getParent().getPrimaryKey() == null) {
                output.reportFailure(new AISValidationFailure(
                        new JoinParentNoExplicitPK (join.getParent().getName())));
                continue;
            }
            TableIndex parentPK= join.getParent().getPrimaryKey().getIndex();
            if (parentPK.getKeyColumns().size() != join.getJoinColumns().size()) {
                output.reportFailure(new AISValidationFailure(
                        new JoinColumnMismatchException (join.getJoinColumns().size(),
                                join.getChild().getName(),
                                join.getParent().getName(),
                                parentPK.getKeyColumns().size())));

                continue;
            }
            Iterator<JoinColumn>  joinColumns = join.getJoinColumns().iterator();
            for (IndexColumn parentPKColumn : parentPK.getKeyColumns()) {
                JoinColumn joinColumn = joinColumns.next();
                if (parentPKColumn.getColumn() != joinColumn.getParent()) {
                    output.reportFailure(new AISValidationFailure (
                            new JoinToWrongColumnsException (
                                    join.getChild().getName(), 
                                    joinColumn.getParent().getName(), 
                                    parentPK.getTable().getName(), parentPKColumn.getColumn().getName())));
                }
            }
        }
    }
}
