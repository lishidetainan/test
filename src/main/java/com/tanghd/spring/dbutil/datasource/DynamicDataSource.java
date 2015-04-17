package com.tanghd.spring.dbutil.datasource;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

public class DynamicDataSource extends AbstractRoutingDataSource {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicDataSource.class);

    private Map<Object, Object> dataSources;

    private static final String DEFAULT = "default";

    private static final ThreadLocal<LinkedList<String>> datasourceHolder = new ThreadLocal<LinkedList<String>>() {

        @Override
        protected LinkedList<String> initialValue() {
            return new LinkedList<String>();
        }

    };

    @Override
    public void afterPropertiesSet() {
        if (null == dataSources) {
            throw new IllegalArgumentException("Property 'dataSources' is required");
        }
        if (null == dataSources.get(DEFAULT)) {
            throw new IllegalArgumentException("key:'default' must be setted in Property 'dataSources'");
        }
        this.setDefaultTargetDataSource(dataSources.get(DEFAULT));
        this.setTargetDataSources(dataSources);
        super.afterPropertiesSet();
    }

    public static void use(String key) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("use datasource :" + datasourceHolder.get());
        }
        LinkedList<String> m = datasourceHolder.get();
        m.offerFirst(key);
    }

    public static void reset() {
        LinkedList<String> m = datasourceHolder.get();
        if (LOG.isDebugEnabled()) {
            LOG.debug("reset datasource {}",m);
        }
        if (m.size() > 0) {
            m.poll();
        }
    }

    @Override
    protected Object determineCurrentLookupKey() {
        LinkedList<String> m = datasourceHolder.get();
        String key =  m.peekFirst() == null ? "" : m.peekFirst();
        if (LOG.isDebugEnabled()) {
            LOG.debug("currenty datasource :" + key);
        }
        
        return key;
    }

    public Map<Object, Object> getDataSources() {
        return dataSources;
    }

    public void setDataSources(Map<Object, Object> dataSources) {
        this.dataSources = dataSources;
    }

}
