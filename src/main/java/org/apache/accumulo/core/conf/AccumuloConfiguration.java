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

import java.util.Iterator;
import java.util.Map.Entry;

public abstract class AccumuloConfiguration implements Iterable<Entry<String, String>> {

    public final cloudbase.core.conf.CBConfiguration impl;

    protected AccumuloConfiguration(cloudbase.core.conf.CBConfiguration impl) {
        this.impl = impl;
    }

    public abstract String get(Property property);

    public abstract Iterator<Entry<String, String>> iterator();

    public long getMemoryInBytes(Property property) {
        return impl.getMemoryInBytes(cloudbaseProperty(property));
    }

    public long getTimeInMillis(Property property) {
        return impl.getTimeInMillis(cloudbaseProperty(property));
    }

    public double getFraction(Property property) {
        return impl.getFraction(cloudbaseProperty(property));
    }

    public double getFraction(String str) {
        if (str.charAt(str.length() - 1) == '%')
            return Double.parseDouble(str.substring(0, str.length() - 1)) / 100.0;
        return Double.parseDouble(str);
    }

    public int getPort(Property property) {
        return impl.getPort(cloudbaseProperty(property));
    }

    public int getCount(Property property) {
        return impl.getCount(cloudbaseProperty(property));
    }

    public static synchronized DefaultConfiguration getDefaultConfiguration() {
        return new DefaultConfiguration(cloudbase.core.conf.CBConfiguration.getDefaultConfiguration());
    }

    protected cloudbase.core.conf.Property cloudbaseProperty(Property property) {
        return cloudbase.core.conf.Property.valueOf(property.name());
    }
}
