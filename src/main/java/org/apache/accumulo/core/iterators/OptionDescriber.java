/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.iterators;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface OptionDescriber {
    public static class IteratorOptions {
        public cloudbase.core.iterators.OptionDescriber.IteratorOptions impl;

        public IteratorOptions(cloudbase.core.iterators.OptionDescriber.IteratorOptions impl) {
            this.impl = impl;
        }

        public IteratorOptions(String name, String description, Map<String, String> namedOptions, List<String> unnamedOptionDescriptions) {
            this.impl = new cloudbase.core.iterators.OptionDescriber.IteratorOptions(
                    name, description, namedOptions, unnamedOptionDescriptions);
        }

        public Map<String, String> getNamedOptions() {
            return impl.namedOptions;
        }

        public List<String> getUnnamedOptionDescriptions() {
            return impl.unnamedOptionDescriptions;
        }

        public String getName() {
            return impl.name;
        }

        public String getDescription() {
            return impl.description;
        }

        public void setNamedOptions(Map<String, String> namedOptions) {
            impl = new IteratorOptions(impl.name, impl.description, namedOptions, impl.unnamedOptionDescriptions).impl;
        }

        public void setUnnamedOptionDescriptions(List<String> unnamedOptionDescriptions) {
            impl = new IteratorOptions(impl.name, impl.description, impl.namedOptions, unnamedOptionDescriptions).impl;
        }

        public void setName(String name) {
            impl = new IteratorOptions(name, impl.description, impl.namedOptions, impl.unnamedOptionDescriptions).impl;
        }

        public void setDescription(String description) {
            impl = new IteratorOptions(impl.name, description, impl.namedOptions, impl.unnamedOptionDescriptions).impl;
        }

        public void addNamedOption(String name, String description) {
            if (impl.namedOptions == null) {
                Map<String, String> namedOptions = new LinkedHashMap<String, String>();
                namedOptions.put(name, description);
                impl = new IteratorOptions(impl.name, impl.description, namedOptions, impl.unnamedOptionDescriptions).impl;
            } else {
                impl.namedOptions.put(name, description);
            }
        }

        public void addUnnamedOption(String description) {
            if (impl.unnamedOptionDescriptions == null) {
                List<String> unnamedOptionDescriptions = new ArrayList<String>();
                unnamedOptionDescriptions.add(description);
                impl = new IteratorOptions(impl.name, impl.description, impl.namedOptions, unnamedOptionDescriptions).impl;
            } else {
                impl.unnamedOptionDescriptions.add(description);
            }
        }
    }

    public IteratorOptions describeOptions();

    public boolean validateOptions(Map<String, String> options);
}
