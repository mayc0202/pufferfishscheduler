package com.pufferfishscheduler.plugin.common;

import java.util.Objects;

public class Param implements Cloneable {
    private String name;
    private String value;
    private String description;

    public Param() {
    }

    public Param(String name, String value, String description) {
        this.name = name;
        this.value = value;
        this.description = description;
    }

    @Override
    public Param clone() {
        try {
            return (Param) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone Param", e);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Param param = (Param) obj;
        return Objects.equals(name, param.name) &&
                Objects.equals(value, param.value) &&
                Objects.equals(description, param.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, description);
    }

    @Override
    public String toString() {
        return "Param{name='" + name + "', value='" + value + "', description='" + description + "'}";
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
