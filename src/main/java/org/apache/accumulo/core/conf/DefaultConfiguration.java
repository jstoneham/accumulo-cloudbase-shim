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
package org.apache.accumulo.core.conf;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map.Entry;

public class DefaultConfiguration extends AccumuloConfiguration {

    public DefaultConfiguration(cloudbase.core.conf.DefaultConfiguration impl) {
        super(impl);
    }

    public static DefaultConfiguration getInstance() {
        return new DefaultConfiguration(cloudbase.core.conf.DefaultConfiguration.getInstance());
    }

    @Override
    public String get(Property property) {
        return impl.get(cloudbase.core.conf.Property.valueOf(property.name()));
    }

    @Override
    public Iterator<Entry<String, String>> iterator() {
        return impl.iterator();
    }

    private static void generateDocumentation(PrintStream doc) {
        throw new UnsupportedOperationException();
    }

    public static void main(String[] args) {
        cloudbase.core.conf.DefaultConfiguration.main(args);
    }
}
