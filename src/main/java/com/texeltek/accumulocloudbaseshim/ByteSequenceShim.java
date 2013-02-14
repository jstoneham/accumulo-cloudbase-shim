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
package com.texeltek.accumulocloudbaseshim;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Transformer;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ByteSequenceShim extends ByteSequence {

    public ByteSequenceShim(cloudbase.core.data.ByteSequence impl) {
        super(impl);
    }

    @Override
    public byte byteAt(int i) {
        return impl.byteAt(i);
    }

    @Override
    public int length() {
        return impl.length();
    }

    @Override
    public ByteSequence subSequence(int i, int i1) {
        return new ByteSequenceShim(impl.subSequence(i, i1));
    }

    @Override
    public byte[] toArray() {
        return impl.toArray();
    }

    @Override
    public boolean isBackedByArray() {
        return impl.isBackedByArray();
    }

    @Override
    public byte[] getBackingArray() {
        return impl.getBackingArray();
    }

    @Override
    public int offset() {
        return impl.offset();
    }

    @SuppressWarnings("unchecked")
    public static Set<cloudbase.core.data.ByteSequence> cloudbaseSet(Set<ByteSequence> byteSequenceSet) {
        return Collections.unmodifiableSet(new HashSet<cloudbase.core.data.ByteSequence>(cloudbaseCollection(byteSequenceSet)));
    }

    @SuppressWarnings("unchecked")
    public static Collection<cloudbase.core.data.ByteSequence> cloudbaseCollection(Collection<ByteSequence> byteSequenceSet) {
        return CollectionUtils.collect(byteSequenceSet,
                new Transformer() {
                    @Override
                    public Object transform(Object o) {
                        return ((ByteSequence) o).impl;
                    }
                });
    }
}
