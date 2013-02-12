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
package org.apache.accumulo.core.iterators.user;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Map;

public class RegExFilter extends Filter {
    public static final String ROW_REGEX = "rowRegex";
    public static final String COLF_REGEX = "colfRegex";
    public static final String COLQ_REGEX = "colqRegex";
    public static final String VALUE_REGEX = "valueRegex";
    public static final String OR_FIELDS = "orFields";
    public static final String ENCODING = "encoding";

    public final cloudbase.core.iterators.filter.RegExFilter impl;

    public RegExFilter() {
        super(new cloudbase.core.iterators.filter.RegExFilter());
        this.impl = (cloudbase.core.iterators.filter.RegExFilter) super.impl;
    }

    public RegExFilter(cloudbase.core.iterators.filter.RegExFilter impl) {
        super(impl);
        this.impl = impl;
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        impl.init(options);
    }

    @Override
    public boolean accept(Key k, Value v) {
        return impl.accept(k.impl, v.impl);
    }

    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptions(impl.describeOptions());
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        return impl.validateOptions(options);
    }
}
