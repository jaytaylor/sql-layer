
package com.akiban.server.entity.model;

import com.akiban.util.ArgumentValidation;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public final class Validation implements Comparable<Validation> {

    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Validation that = (Validation) o;
        return name.equals(that.name) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s: %s", name, value);
    }

    @Override
    public int compareTo(Validation o) {
        int keyCompares = name.compareTo(o.name);
        if (keyCompares != 0)
            return keyCompares;
        if (value == null)
            return o.value == null ? 0 : -1;
        else if (o.value == null)
            return 1;
        String valueString = value.toString();
        String oValueString = o.value.toString();
        return valueString.compareTo(oValueString);
    }

    public static Set<Validation> createValidations(Collection<Map<String, ?>> validations) {
        Set<Validation> result = new TreeSet<>();
        for (Map<String, ?> validation : validations) {
            if (!result.add(new Validation(validation)))
                throw new IllegalEntityDefinition("duplicate validation:" + validation);
        }
        return Collections.unmodifiableSet(result);
    }

    Validation(Map<String, ?> validation) {
        if (validation.size() != 1)
            throw new IllegalEntityDefinition("illegal validation definition (map must have one entry)");
        Map.Entry<String, ?> entry = validation.entrySet().iterator().next();
        this.name = entry.getKey();
        this.value = entry.getValue();
    }

    // for testing
    public Validation(String name, Object value) {
        ArgumentValidation.notNull("validation name", name);
        this.name = name;
        this.value = value;
    }

    private final String name;
    private final Object value;
}
