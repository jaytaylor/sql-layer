
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.GroupHasMultipleRootsException;

class GroupSingleRoot implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Group group : ais.getGroups().values()) {
            validateGroup (ais, group, output);
        }
    }
    
    private void validateGroup (AkibanInformationSchema ais, Group group, AISValidationOutput output) {
        UserTable root = null;
        for (UserTable userTable : ais.getUserTables().values()) {
            if (userTable.getGroup() == group && userTable.isRoot()) {
                if (root == null) {
                    root = userTable;
                } else {
                    output.reportFailure(new AISValidationFailure (
                            new GroupHasMultipleRootsException(group.getName(), root.getName(), userTable.getName())));
                }
            }
        }
    }
}
