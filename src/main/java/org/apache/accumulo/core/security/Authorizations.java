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
package org.apache.accumulo.core.security;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class Authorizations implements Iterable<byte[]>, Serializable {

    public final cloudbase.core.security.Authorizations impl;

    private static final boolean[] validAuthChars = new boolean[256];

    static {
        for (int i = 0; i < 256; i++) {
            validAuthChars[i] = false;
        }

        for (int i = 'a'; i <= 'z'; i++) {
            validAuthChars[i] = true;
        }

        for (int i = 'A'; i <= 'Z'; i++) {
            validAuthChars[i] = true;
        }

        for (int i = '0'; i <= '9'; i++) {
            validAuthChars[i] = true;
        }

        validAuthChars['_'] = true;
        validAuthChars['-'] = true;
        validAuthChars[':'] = true;
    }

    static final boolean isValidAuthChar(byte b) {
        return validAuthChars[0xff & b];
    }

    public Authorizations(Collection<byte[]> authorizations) {
        this.impl = new cloudbase.core.security.Authorizations(authorizations);
    }

    public Authorizations(byte[] authorizations) {
        this.impl = new cloudbase.core.security.Authorizations(authorizations);
    }

    public Authorizations() {
        this.impl = new cloudbase.core.security.Authorizations();
    }

    public Authorizations(String... authorizations) {
        this.impl = new cloudbase.core.security.Authorizations(authorizations);
    }

    public Authorizations(cloudbase.core.security.Authorizations impl) {
        this.impl = impl;
    }

    public byte[] getAuthorizationsArray() {
        return impl.getAuthorizationsArray();
    }

    public List<byte[]> getAuthorizations() {
        return impl.getAuthorizations();
    }

    public String toString() {
        return impl.toString();
    }

    public boolean contains(byte[] auth) {
        return impl.contains(auth);
    }

    public boolean equals(Object o) {
        if (!(o instanceof Authorizations)) {
            return false;
        }
        return impl.equals(((Authorizations) o).impl);
    }

    public int hashCode() {
        return impl.hashCode();
    }

    public int size() {
        return impl.size();
    }

    public boolean isEmpty() {
        return impl.isEmpty();
    }

    public Iterator<byte[]> iterator() {
        return impl.iterator();
    }

    public String serialize() {
        return impl.serialize();
    }
}
