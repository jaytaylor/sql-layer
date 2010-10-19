package com.akiban.cserver.service.config;

import com.akiban.util.ArgumentValidation;

public final class Property implements Comparable<Property> {

    public static final class Key implements Comparable<Key> {
        private final String module;
        private final String name;

        public Key(String module, String name) {
            ArgumentValidation.notNull("module", module);
            ArgumentValidation.notNull("property name", name);
            this.module = module;
            this.name = name;
        }

        public String getModule() {
            return module;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Key that = (Key) o;

            if (!module.equals(that.module)) return false;
            return name.equals(that.name);

        }

        @Override
        public int hashCode() {
            int result = module.hashCode();
            result = 31 * result + name.hashCode();
            return result;
        }

        @Override
        public int compareTo(Key other) {
            int comparison = this.module.compareTo(other.module);
            if (comparison != 0) {
                return comparison;
            }
            return this.name.compareTo(other.name);
        }
    }

    public static Key parseKey(String fullName) {
        ArgumentValidation.notNull("property name", fullName);
        int dotAt = fullName.indexOf('.');
        final String beforeDot;
        final String afterDot;
        if (dotAt < 0) {
            beforeDot = "";
            afterDot = fullName;
        }
        else {
            beforeDot = fullName.substring(0, dotAt);
            afterDot = fullName.substring(dotAt+1);
        }
        return new Key(beforeDot, afterDot);
    }

    private final Key key;
    private final String value;

    Property(Key key, String value) {
        this.key = key;
        this.value = value;
    }

    Property(String module, String name, String value) {
        this.key = new Key(module, name);
        this.value = value;
    }

    public String getModule() {
        return key.getModule();
    }

    public String getName() {
        return key.getName();
    }

    public String getValue() {
        return value;
    }

    public Key getKey() {
        return key;
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof Property) && key.equals(((Property)obj).key);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public int compareTo(Property o) {
        return key.compareTo(o.key);
    }

    @Override
    public String toString() {
        return String.format("Property[%s > %s = %s]", getModule(), getName(), value);
    }
}
