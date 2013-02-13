package com.texeltek.accumulocloudbaseshim;

import org.apache.accumulo.core.client.impl.TabletLocator;

public class TabletLocatorShim extends TabletLocator {

    public cloudbase.core.client.impl.TabletLocator impl;

    public TabletLocatorShim(cloudbase.core.client.impl.TabletLocator impl) {
        this.impl = impl;
    }
}
