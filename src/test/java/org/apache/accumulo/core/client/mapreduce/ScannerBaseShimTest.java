package org.apache.accumulo.core.client.mapreduce;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.iterators.SortedKeyIterator;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.core.iterators.user.ColumnAgeOffFilter;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ScannerBaseShimTest {

    private Scanner scanner;

    @Before
    public void setup() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
        Instance instance = new MockInstance();
        Connector connector = instance.getConnector("user", "password".getBytes());
        String tableName = "table";
        connector.tableOperations().create(tableName);
        this.scanner = connector.createScanner(tableName, new Authorizations());
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testAgeOffFilterLoads() {
        IteratorSetting setting = new IteratorSetting(10, "ageoff", AgeOffFilter.class);
        AgeOffFilter.setTTL(setting, 500l);
        scanner.addScanIterator(setting);
        scanner.iterator().hasNext();
    }

    @Test
    public void testColumnAgeOffFilterLoads() {
        IteratorSetting setting = new IteratorSetting(10, "columnageoff", ColumnAgeOffFilter.class);
        setting.addOption("ttl", "500");
        scanner.addScanIterator(setting);
        scanner.iterator().hasNext();
    }

    @Test
    public void testRegexFilterLoads() {
        IteratorSetting setting = new IteratorSetting(10, "regex", RegExFilter.class);
        scanner.addScanIterator(setting);
        scanner.iterator().hasNext();
    }

    @Test
    public void testTranslateIteratorsUserPackageLoads() {
        IteratorSetting setting = new IteratorSetting(10, "wholerow", WholeRowIterator.class);
        scanner.addScanIterator(setting);
        scanner.iterator().hasNext();
    }

    @Test
    public void testTranslateIteratorsPackageLoads() {
        IteratorSetting setting = new IteratorSetting(10, "sortedkey", SortedKeyIterator.class);
        scanner.addScanIterator(setting);
        scanner.iterator().hasNext();
    }
}
