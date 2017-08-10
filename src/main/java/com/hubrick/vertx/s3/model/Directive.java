/**
 * Copyright (C) 2016 Etaia AS (oss@hubrick.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hubrick.vertx.s3.model;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Emir Dizdarevic
 * @since 3.1.0
 */
public enum Directive {
    COPY,
    REPLACE;

    private static Map<String, Directive> REVERSE_LOOKUP = new HashMap<>();
    static {
        for(Directive directive : values()) {
            REVERSE_LOOKUP.put(directive.name(), directive);
        }
    }

    public static Directive fromString(String value) {
        return REVERSE_LOOKUP.get(value);
    }

    @Override
    public String toString() {
        return name();
    }
}
