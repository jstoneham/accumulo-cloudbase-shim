package cloudbase.core.client.mock;

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.Connector;
import cloudbase.core.conf.CBConfiguration;

import java.util.List;

/**
 */
public class MockInstanceShim extends MockInstance {

    private final MockInstance impl;

    public MockInstanceShim(MockInstance mockInstance) {
        super();
        this.impl = mockInstance;
    }

    public MockInstanceShim(String instanceName) {
        this(new MockInstance(instanceName));
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
    public Connector getConnector(String user, byte[] pass) throws CBException, CBSecurityException {
        return new MockConnectorShim(user, impl);
    }

    @Override
    public Connector getConnector(String user, CharSequence pass) throws CBException, CBSecurityException {
        return new MockConnectorShim(user, impl);
    }

    @Override
    public CBConfiguration getConfiguration() {
        return impl.getConfiguration();
    }

    @Override
    public void setConfiguration(CBConfiguration conf) {
        impl.setConfiguration(conf);
    }
}
