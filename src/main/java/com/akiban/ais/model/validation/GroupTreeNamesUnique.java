
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.DuplicateGroupTreeNamesException;

import java.util.HashMap;
import java.util.Map;

class GroupTreeNamesUnique implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        Map<String,Group> treeNameMap = new HashMap<>();

        for(Group group : ais.getGroups().values()) {
            String treeName = group.getTreeName();
            Group curGroup = treeNameMap.put(treeName, group);
            if(curGroup != null) {
                UserTable root = group.getRoot();
                UserTable curRoot = curGroup.getRoot();
                output.reportFailure(
                    new AISValidationFailure(
                            new DuplicateGroupTreeNamesException(root.getName(), curRoot.getName(), treeName)));
            }
        }
    }
}
