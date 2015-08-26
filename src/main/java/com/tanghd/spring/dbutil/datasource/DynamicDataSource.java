package com.tanghd.spring.dbutil.datasource;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

public class DynamicDataSource extends AbstractRoutingDataSource {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicDataSource.class);

    private DataSource master;
    private List<DataSource> slaves;
    private AtomicLong slaveCount = new AtomicLong();
    private int slaveSize = 0;

    private Map<Object, Object> dataSources = new HashMap<Object, Object>();

    private static final String DEFAULT = "master";
    private static final String SLAVE = "slave";

    private static final ThreadLocal<LinkedList<String>> datasourceHolder = new ThreadLocal<LinkedList<String>>() {

        @Override
        protected LinkedList<String> initialValue() {
            return new LinkedList<String>();
        }

    };

    @Override
    public void afterPropertiesSet() {
        if (null == master) {
            throw new IllegalArgumentException("Property 'master' is required");
        }
        dataSources.put(DEFAULT, master);
        if (null != slaves && slaves.size() > 0) {
            for (int i = 0; i < slaves.size(); i++) {
                dataSources.put(SLAVE + (i + 1), slaves.get(i));
            }
            slaveSize = slaves.size();
        }
        this.setDefaultTargetDataSource(master);
        this.setTargetDataSources(dataSources);
        super.afterPropertiesSet();
    }

    public static void useMaster() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("use datasource :" + datasourceHolder.get());
        }
        LinkedList<String> m = datasourceHolder.get();
        m.offerFirst(DEFAULT);
    }

    public static void useSlave() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("use datasource :" + datasourceHolder.get());
        }
        LinkedList<String> m = datasourceHolder.get();
        m.offerFirst(SLAVE);
    }

    public static void reset() {
        LinkedList<String> m = datasourceHolder.get();
        if (LOG.isDebugEnabled()) {
            LOG.debug("reset datasource {}", m);
        }
        if (m.size() > 0) {
            m.poll();
        }
    }

    @Override
    protected Object determineCurrentLookupKey() {
        LinkedList<String> m = datasourceHolder.get();
        String key = m.peekFirst() == null ? "" : m.peekFirst();
        if (LOG.isDebugEnabled()) {
            LOG.debug("currenty datasource :" + key);
        }
        if (null != key) {
            if (DEFAULT.equals(key)) {
                return key;
            } else if (SLAVE.equals(key)) {
                if (slaveSize > 1) {//Slave loadBalance
                    long c = slaveCount.incrementAndGet();
                    c = c % slaveSize;
                    return SLAVE + (c + 1);
                } else {
                    return SLAVE + "1";
                }
            }
            return null;
        } else {
            return null;
        }
    }

    public DataSource getMaster() {
        return master;
    }

    public List<DataSource> getSlaves() {
        return slaves;
    }

    public void setMaster(DataSource master) {
        this.master = master;
    }

    public void setSlaves(List<DataSource> slaves) {
        this.slaves = slaves;
    }

}
