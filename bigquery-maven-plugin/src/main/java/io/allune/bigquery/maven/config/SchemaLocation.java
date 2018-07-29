/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.allune.bigquery.maven.config;

import io.allune.bigquery.maven.ConfigurationException;

public final class SchemaLocation implements Comparable<SchemaLocation> {
    private static final String CLASSPATH_PREFIX = "classpath:";
    private static final String FILESYSTEM_PREFIX = "file:";

    private String prefix;
    private String path;

    SchemaLocation(String descriptor) {
        String normalizedDescriptor = descriptor.trim().replace("\\", "/");

        if (normalizedDescriptor.contains(":")) {
            prefix = normalizedDescriptor.substring(0, normalizedDescriptor.indexOf(':') + 1);
            path = normalizedDescriptor.substring(normalizedDescriptor.indexOf(':') + 1);
        } else {
            prefix = CLASSPATH_PREFIX;
            path = normalizedDescriptor;
        }

        if (isClassPath()) {
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
        } else {
            if (!isFileSystem()) {
                throw new ConfigurationException("Unknown schema location prefix: " + normalizedDescriptor + ". " +
                        "file: or classpath: only supported");
            }
        }

        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
    }

    public String getPrefix() {
        return prefix;
    }

    public String getPath() {
        return path;
    }

    boolean isClassPath() {
        return CLASSPATH_PREFIX.equals(prefix);
    }

    private boolean isFileSystem() {
        return FILESYSTEM_PREFIX.equals(prefix);
    }

    public boolean isParentOf(SchemaLocation other) {
        return (other.getDescriptor() + "/").startsWith(getDescriptor() + "/");
    }

    public String getDescriptor() {
        return prefix + path;
    }

    public int compareTo(SchemaLocation o) {
        return getDescriptor().compareTo(o.getDescriptor());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaLocation schemaLocation = (SchemaLocation) o;

        return getDescriptor().equals(schemaLocation.getDescriptor());
    }

    @Override
    public int hashCode() {
        return getDescriptor().hashCode();
    }

    @Override
    public String toString() {
        return getDescriptor();
    }
}