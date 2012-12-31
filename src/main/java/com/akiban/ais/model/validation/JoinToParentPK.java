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
