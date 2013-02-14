package com.texeltek.accumulocloudbaseshim;

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;

import java.util.List;

public class InstanceShim implements Instance {

    public final cloudbase.core.client.Instance impl;

    public InstanceShim(cloudbase.core.client.Instance impl) {
        this.impl = impl;
    }

    @Override
    public String getRootTabletLocation() {
        return impl.getRootTabletLocation();
    }

    @Override
    public List<String> getMasterLocations() {
        return impl.getMasterLocations();
    }

    @Override
    public String getInstanceID() {
        return impl.getInstanceID();
    }

    @Override
    public String getInstanceName() {
        return impl.getInstanceName();
    }

    @Override
    public String getZooKeepers() {
        return impl.getZooKeepers();
    }

    @Override
    public int getZooKeepersSessionTimeOut() {
        return impl.getZooKeepersSessionTimeOut();
    }

    @Override
    public Connector getConnector(String user, byte[] pass) throws AccumuloException, AccumuloSecurityException {
        try {
            return new Connector(impl.getConnector(user, pass));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    @Override
    public cloudbase.core.client.Connector getNativeConnector(String user, byte[] pass) throws CBException, CBSecurityException {
        return impl.getConnector(user, pass);
    }

    @Override
    public Connector getConnector(String user, CharSequence pass) throws AccumuloException, AccumuloSecurityException {
        try {
            return new Connector(impl.getConnector(user, pass));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    @Override
    public cloudbase.core.client.Connector getNativeConnector(String user, CharSequence pass) throws CBException, CBSecurityException {
        return impl.getConnector(user, pass);
    }

    @Override
    public AccumuloConfiguration getConfiguration() {
        return new AccumuloConfigurationShim(impl.getConfiguration());
    }

    @Override
    public void setConfiguration(AccumuloConfiguration conf) {
        impl.setConfiguration(conf.impl);
    }
}
