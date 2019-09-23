package qf.Class.sparksqlday04;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.tools.ConsoleConsumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MyKafkaConsumer {
    private static final String topic="gp23";
    private static final Integer threads=2;
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("zookeeper.connect","10.0.88.242:2181/kafka");
        //设置组别信息
        properties.put("group.id","gp23");
        //消费位置
        //	largest,从最近位置，消费者启动后产生的数据开始消费
        // smallest,从第一条数据开始消费
        properties.put("auto.offset.reset","smallest");
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        //指定消费的topic信息
        HashMap<String, Integer> hashMap = new HashMap<>();
        hashMap.put(topic,threads);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumerConnector.createMessageStreams(hashMap);
        //解析消息
        //获取相应topic对应的消息流
        List<KafkaStream<byte[], byte[]>> kafkaStreams = messageStreams.get(topic);
        for(KafkaStream<byte[], byte[]>kafkastream:kafkaStreams){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for(MessageAndMetadata<byte[], byte[]> mm:kafkastream){
                        System.out.println(new String(mm.message()));
                    }
                }
            }).start();

        }




    }
}
