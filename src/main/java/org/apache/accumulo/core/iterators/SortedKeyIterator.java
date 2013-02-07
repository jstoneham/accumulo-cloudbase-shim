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

import com.texeltek.accumulocloudbaseshim.IteratorEnvironmentShim;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.Map;

public class SortedKeyIterator extends WrappingIterator implements OptionDescriber {

    private static final Value NOVALUE = new Value(new byte[0]);

    public final cloudbase.core.iterators.SortedKeyIterator impl;

    public SortedKeyIterator() {
        super(new cloudbase.core.iterators.SortedKeyIterator());
        this.impl = (cloudbase.core.iterators.SortedKeyIterator) super.impl;
    }

    public SortedKeyIterator(SortedKeyIterator other, IteratorEnvironment env) {
        super(new cloudbase.core.iterators.SortedKeyIterator(other.impl, ((IteratorEnvironmentShim) env).impl));
        this.impl = (cloudbase.core.iterators.SortedKeyIterator) super.impl;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        return new SortedKeyIterator(this, env);
    }

    @Override
    public Value getTopValue() {
        return NOVALUE;
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
