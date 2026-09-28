package org.apache.cassandra.analytics;

public class BtiBulkReaderSaiTest extends BulkReaderSaiTest
{
    @Override
    protected String sstableFormat()
    {
        return "bti";
    }
}
