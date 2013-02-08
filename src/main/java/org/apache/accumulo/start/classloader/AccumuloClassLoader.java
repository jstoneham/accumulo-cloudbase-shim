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
package org.apache.accumulo.start.classloader;

public class AccumuloClassLoader {

    public static final String CLASSPATH_PROPERTY_NAME = "general.classpaths";

    public static final String DYNAMIC_CLASSPATH_PROPERTY_NAME = "general.dynamic.classpaths";

    public static final String ACCUMULO_CLASSPATH_VALUE = "$ACCUMULO_HOME/conf,\n" + "$ACCUMULO_HOME/lib/[^.].$ACCUMULO_VERSION.jar,\n"
            + "$ACCUMULO_HOME/lib/[^.].*.jar,\n" + "$ZOOKEEPER_HOME/zookeeper[^.].*.jar,\n" + "$HADOOP_HOME/[^.].*.jar,\n" + "$HADOOP_HOME/conf,\n"
            + "$HADOOP_HOME/lib/[^.].*.jar,\n";

    public static final String DEFAULT_DYNAMIC_CLASSPATH_VALUE = "$ACCUMULO_HOME/lib/ext/[^.].*.jar\n";

    public static final String DEFAULT_CLASSPATH_VALUE = ACCUMULO_CLASSPATH_VALUE;
}
