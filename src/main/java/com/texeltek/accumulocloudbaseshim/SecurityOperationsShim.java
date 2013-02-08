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

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;

import java.util.Set;

public class SecurityOperationsShim implements SecurityOperations {
    public final cloudbase.core.client.admin.SecurityOperations impl;

    public SecurityOperationsShim(cloudbase.core.client.admin.SecurityOperations impl) {
        this.impl = impl;
    }

    public void createUser(String user, byte[] password, Authorizations authorizations) throws AccumuloException, AccumuloSecurityException {
        try {
            impl.createUser(user, password, authorizations.impl);
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public void dropUser(String user) throws AccumuloException, AccumuloSecurityException {
        try {
            impl.dropUser(user);
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public boolean authenticateUser(String user, byte[] password) throws AccumuloException, AccumuloSecurityException {
        try {
            return impl.authenticateUser(user, password);
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public void changeUserPassword(String user, byte[] password) throws AccumuloException, AccumuloSecurityException {
        try {
            impl.changeUserPassword(user, password);
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public void changeUserAuthorizations(String user, Authorizations authorizations) throws AccumuloException, AccumuloSecurityException {
        try {
            impl.changeUserAuthorizations(user, authorizations.impl);
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public Authorizations getUserAuthorizations(String user) throws AccumuloException, AccumuloSecurityException {
        try {
            return new Authorizations(impl.getUserAuthorizations(user));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public boolean hasSystemPermission(String user, SystemPermission perm) throws AccumuloException, AccumuloSecurityException {
        try {
            return impl.hasSystemPermission(user, cloudbase.core.security.SystemPermission.valueOf(perm.name()));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public boolean hasTablePermission(String user, String table, TablePermission perm) throws AccumuloException, AccumuloSecurityException {
        try {
            return impl.hasTablePermission(user, table, cloudbase.core.security.TablePermission.valueOf(perm.name()));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public void grantSystemPermission(String user, SystemPermission permission) throws AccumuloException, AccumuloSecurityException {
        try {
            impl.grantSystemPermission(user, cloudbase.core.security.SystemPermission.valueOf(permission.name()));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public void grantTablePermission(String user, String table, TablePermission permission) throws AccumuloException, AccumuloSecurityException {
        try {
            impl.grantTablePermission(user, table, cloudbase.core.security.TablePermission.valueOf(permission.name()));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public void revokeSystemPermission(String user, SystemPermission permission) throws AccumuloException, AccumuloSecurityException {
        try {
            impl.revokeSystemPermission(user, cloudbase.core.security.SystemPermission.valueOf(permission.name()));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public void revokeTablePermission(String user, String table, TablePermission permission) throws AccumuloException, AccumuloSecurityException {
        try {
            impl.revokeTablePermission(user, table, cloudbase.core.security.TablePermission.valueOf(permission.name()));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public Set<String> listUsers() throws AccumuloException, AccumuloSecurityException {
        try {
            return impl.listUsers();
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public String toString() {
        return impl.toString();
    }
}
