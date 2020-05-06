package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.MemorySize;

import java.util.Map;

/**
 * Created by yuchunfan on 2020/4/29.
 */
@PublicEvolving
public class EttHBase extends ConnectorDescriptor {
    private DescriptorProperties properties = new DescriptorProperties();

    public EttHBase() {
        super("hbase", 1, false);
    }

    public EttHBase tableName(String tableName) {
        this.properties.putString("connector.table-name", tableName);
        return this;
    }

    public EttHBase zookeeperQuorum(String zookeeperQuorum) {
        this.properties.putString("connector.zookeeper.quorum", zookeeperQuorum);
        return this;
    }

    public EttHBase zookeeperNodeParent(String zookeeperNodeParent) {
        this.properties.putString("connector.zookeeper.znode.parent", zookeeperNodeParent);
        return this;
    }

    public EttHBase writeBufferFlushMaxSize(String maxSize) {
        this.properties.putMemorySize("connector.write.buffer-flush.max-size", MemorySize.parse(maxSize, MemorySize.MemoryUnit.BYTES));
        return this;
    }

    public EttHBase writeBufferFlushMaxRows(int writeBufferFlushMaxRows) {
        this.properties.putInt("connector.write.buffer-flush.max-rows", writeBufferFlushMaxRows);
        return this;
    }

    public EttHBase writeBufferFlushInterval(String interval) {
        this.properties.putString("connector.write.buffer-flush.interval", interval);
        return this;
    }

    protected Map<String, String> toConnectorProperties() {
        return this.properties.asMap();
    }
}
