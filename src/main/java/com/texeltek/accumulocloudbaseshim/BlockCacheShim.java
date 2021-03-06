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

import org.apache.accumulo.core.file.blockfile.cache.BlockCache;

public class BlockCacheShim implements BlockCache {

    public final cloudbase.core.file.blockfile.cache.BlockCache impl;

    public BlockCacheShim(cloudbase.core.file.blockfile.cache.BlockCache impl) {
        this.impl = impl;
    }

    @Override
    public void cacheBlock(String blockName, byte[] buf, boolean inMemory) {
        impl.cacheBlock(blockName, buf, inMemory);
    }

    @Override
    public void cacheBlock(String blockName, byte[] buf) {
        impl.cacheBlock(blockName, buf);
    }

    @Override
    public byte[] getBlock(String blockName) {
        return impl.getBlock(blockName);
    }

    @Override
    public void shutdown() {
        impl.shutdown();
    }
}
