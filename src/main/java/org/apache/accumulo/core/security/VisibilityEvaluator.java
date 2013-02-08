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

import java.util.Collection;

public class VisibilityEvaluator {

    private final cloudbase.core.security.VisibilityEvaluator impl;
    private final Authorizations auths;

    public VisibilityEvaluator(Collection<byte[]> authorizations) {
        this.auths = new Authorizations(new cloudbase.core.security.Authorizations(authorizations));
        this.impl = new cloudbase.core.security.VisibilityEvaluator(auths.impl);
    }

    public VisibilityEvaluator(Authorizations authorizations) {
        this.auths = authorizations;
        this.impl = new cloudbase.core.security.VisibilityEvaluator(authorizations.impl);
    }

    public Authorizations getAuthorizations() {
        return this.auths;
    }

    public boolean evaluate(ColumnVisibility visibility) throws VisibilityParseException {
        try {
            return impl.evaluate(new cloudbase.core.security.ColumnVisibility(visibility.getExpression()));
        } catch (cloudbase.core.security.VisibilityParseException e) {
            throw new VisibilityParseException(e);
        }
    }
}
