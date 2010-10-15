package com.akiban.cserver.service.config;

public final class Property implements Comparable<Property> {
    private final String module;
    private final String name;
    private final String defaultValue;
    private volatile String value;

    Property(String module, String name, String defaultValue) {
        if (module == null) {
            throw new IllegalArgumentException("namespace may not be null");
        }
        if (name== null) {
            throw new IllegalArgumentException("name may not be null");
        }
        if (defaultValue == null) {
            throw new IllegalArgumentException("value may not be null");
        }
        this.module = module;
        this.name = name;
        this.defaultValue = defaultValue;
        this.value = null;
    }

    public String getModule() {
        return module;
    }

    public String getName() {
        return name;
    }

    public String getValueDefault() {
        return defaultValue;
    }

    public String getValue() {
        final String localValue = value;
        return localValue == null ? defaultValue : localValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Property that = (Property) o;

        if (!module.equals(that.module)) return false;
        if (!name.equals(that.name)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = module.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override
    public int compareTo(Property other) {
        int comparison = this.module.compareTo(other.module);
        if (comparison != 0) {
            return comparison;
        }
        return this.name.compareTo(other.name);
    }

    @Override
    public String toString() {
        return String.format("Property[%s > %s = %s", module, name, defaultValue);
    }
}
