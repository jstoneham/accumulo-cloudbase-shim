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
package org.apache.accumulo.core.client;

import cloudbase.core.client.CBSecurityException;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;

public class AccumuloSecurityException extends Exception {
    public final CBSecurityException impl;

    public AccumuloSecurityException(CBSecurityException e) {
        super(e.getMessage(), e);
        impl = e;
    }

    public String getUser() {
        return impl.getUser();
    }

    public String getMessage() {
        return impl.getMessage();
    }

    public SecurityErrorCode getErrorCode() {
        return SecurityErrorCode.valueOf(impl.getErrorCode().name());
    }

    public String toString() {
        return impl.toString();
    }
}
