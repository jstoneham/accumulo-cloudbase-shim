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
package org.apache.accumulo.core.data;

import com.texeltek.accumulocloudbaseshim.ByteSequenceShim;

import java.io.Serializable;

public class ArrayByteSequence extends ByteSequence implements Serializable {

    private static final long serialVersionUID = 1L;

    public ArrayByteSequence(byte data[]) {
        super(new cloudbase.core.data.ArrayByteSequence(data));
    }

    public ArrayByteSequence(byte data[], int offset, int length) {
        super(new cloudbase.core.data.ArrayByteSequence(data, offset, length));
    }

    public ArrayByteSequence(String s) {
        this(s.getBytes());
    }

    public ArrayByteSequence(cloudbase.core.data.ByteSequence bs) {
        super(bs);
    }

    @Override
    public byte byteAt(int i) {
        return impl.byteAt(i);
    }

    @Override
    public byte[] getBackingArray() {
        return impl.getBackingArray();
    }

    @Override
    public boolean isBackedByArray() {
        return impl.isBackedByArray();
    }

    @Override
    public int length() {
        return impl.length();
    }

    @Override
    public int offset() {
        return impl.offset();
    }

    @Override
    public ByteSequence subSequence(int start, int end) {
        return new ByteSequenceShim(impl.subSequence(start, end));
    }

    @Override
    public byte[] toArray() {
        return impl.toArray();
    }

    public String toString() {
        return impl.toString();
    }
}
