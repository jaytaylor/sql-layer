
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.Types;
import com.akiban.server.error.TypesAreStaticException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class TypesAreFromStatic implements AISValidation {
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Type type : ais.getTypes()) {
            String typeName = type.name();
            Type fromStatic = staticTypesByName.get(typeName);
            if (type != fromStatic) {
                output.reportFailure(
                        new AISValidationFailure(
                                new TypesAreStaticException (type)));
            }
        }
    }

    private static Map<String,Type> staticTypesByName() {
        Map<String,Type> results = new HashMap<>();
        for (Type type : Types.types()) {
            results.put(type.name(), type);
        }
        return Collections.unmodifiableMap(results);
    }

    private final static Map<String,Type> staticTypesByName = staticTypesByName();
}
