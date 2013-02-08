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
package org.apache.accumulo.core.client.mock;

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;

import java.util.Set;

public class MockSecurityOperations implements SecurityOperations {

    public final cloudbase.core.client.mock.MockSecurityOperations impl;

    public MockSecurityOperations(cloudbase.core.client.mock.MockSecurityOperations impl) {
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

    public boolean authenticateUser(String name, byte[] password) throws AccumuloException, AccumuloSecurityException {
        try {
            return impl.authenticateUser(name, password);
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public void changeUserPassword(String name, byte[] password) throws AccumuloException, AccumuloSecurityException {
        try {
            impl.changeUserPassword(name, password);
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public void changeUserAuthorizations(String name, Authorizations authorizations) throws AccumuloException, AccumuloSecurityException {
        try {
            impl.changeUserAuthorizations(name, authorizations.impl);
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public Authorizations getUserAuthorizations(String name) throws AccumuloException, AccumuloSecurityException {
        try {
            return new Authorizations(impl.getUserAuthorizations(name));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public boolean hasSystemPermission(String name, SystemPermission perm) throws AccumuloException, AccumuloSecurityException {
        try {
            return impl.hasSystemPermission(name, cloudbase.core.security.SystemPermission.valueOf(perm.name()));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public boolean hasTablePermission(String name, String tableName, TablePermission perm) throws AccumuloException, AccumuloSecurityException {
        try {
            return impl.hasTablePermission(name, tableName, cloudbase.core.security.TablePermission.valueOf(perm.name()));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public void grantSystemPermission(String name, SystemPermission permission) throws AccumuloException, AccumuloSecurityException {
        try {
            impl.grantSystemPermission(name, cloudbase.core.security.SystemPermission.valueOf(permission.name()));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public void grantTablePermission(String name, String tableName, TablePermission permission) throws AccumuloException, AccumuloSecurityException {
        try {
            impl.grantTablePermission(name, tableName, cloudbase.core.security.TablePermission.valueOf(permission.name()));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public void revokeSystemPermission(String name, SystemPermission permission) throws AccumuloException, AccumuloSecurityException {
        try {
            impl.revokeSystemPermission(name, cloudbase.core.security.SystemPermission.valueOf(permission.name()));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public void revokeTablePermission(String name, String tableName, TablePermission permission) throws AccumuloException, AccumuloSecurityException {
        try {
            impl.revokeTablePermission(name, tableName, cloudbase.core.security.TablePermission.valueOf(permission.name()));
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
