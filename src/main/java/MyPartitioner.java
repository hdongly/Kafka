import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyPartitioner implements Partitioner {
    /**
     * 计算某条消息要发送到哪个分区
     * @param topic 主题
     * @param key   消息的key
     * @param keyBytes 消息的key序列化后的字节数组
     * @param value 消息的value
     * @param valueBytes   消息的value序列化后的字节数组
     * @param cluster
     * @return
     *
     * 需求: 以atguigu主题为例，2个分区
     *       消息的 value包含"atguigu"的 进入0号分区
     *       其他的消息进入1号分区
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String msgValue = value.toString();
        int partition ;
        if(msgValue.contains("atguigu")){
            partition = 0;
        }else{
            partition = 1;
        }
        return partition;
    }

    /**
     * 收尾工作
     */
    @Override
    public void close() {

    }

    /**
     * 读取配置的
     * @param configs
     */
    @Override
    public void configure(Map<String, ?> configs) {

    }
}