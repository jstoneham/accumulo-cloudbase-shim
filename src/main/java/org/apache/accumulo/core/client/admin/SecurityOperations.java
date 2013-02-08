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
package org.apache.accumulo.core.client.admin;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;

import java.util.Set;

public interface SecurityOperations {

    public void createUser(String user, byte[] password, Authorizations authorizations) throws AccumuloException, AccumuloSecurityException;

    public void dropUser(String user) throws AccumuloException, AccumuloSecurityException;

    public boolean authenticateUser(String user, byte[] password) throws AccumuloException, AccumuloSecurityException;

    public void changeUserPassword(String user, byte[] password) throws AccumuloException, AccumuloSecurityException;

    public void changeUserAuthorizations(String user, Authorizations authorizations) throws AccumuloException, AccumuloSecurityException;

    public Authorizations getUserAuthorizations(String user) throws AccumuloException, AccumuloSecurityException;

    public boolean hasSystemPermission(String user, SystemPermission perm) throws AccumuloException, AccumuloSecurityException;

    public boolean hasTablePermission(String user, String table, TablePermission perm) throws AccumuloException, AccumuloSecurityException;

    public void grantSystemPermission(String user, SystemPermission permission) throws AccumuloException, AccumuloSecurityException;

    public void grantTablePermission(String user, String table, TablePermission permission) throws AccumuloException, AccumuloSecurityException;

    public void revokeSystemPermission(String user, SystemPermission permission) throws AccumuloException, AccumuloSecurityException;

    public void revokeTablePermission(String user, String table, TablePermission permission) throws AccumuloException, AccumuloSecurityException;

    public Set<String> listUsers() throws AccumuloException, AccumuloSecurityException;
}
