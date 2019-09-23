package qf.Class.sparksqlday04;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class MyKafkaProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //设定kafka服务器的ip,端口号
        properties.put("bootstrap.servers","10.0.88.242:9092");
        //ack=-1:所有主从分区确认收到消息：会发生数据重复
        //ack = 1:leader确认收到消息，消息发送成功：发生丢失，或数据重复
        //ack =0:直接发送下一条消息：数据丢失
        properties.put("ack", "all");
        //消息发送尝试次数
        properties.put("retries",1);
        //发送消息批次大小
        properties.put("batch.size",16384);
        properties.put("buffer.memory",33554432);
        //指定发送消息的序列化方式
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        //创建一个生产者对象
        KafkaProducer kafkaProducer = new KafkaProducer(properties);
        for(int i=0;i<10;i++){
            //生产key,value类型的消息
           // kafkaProducer.send(new ProducerRecord("gp23","kafka","kafka"+i));
            //生产的key：null，只有value的消息
           // kafkaProducer.send(new ProducerRecord("gp23","kafka"+i));
            //加一个回调
            kafkaProducer.send(new ProducerRecord("gp23", "kafka" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(recordMetadata != null){
                        System.out.println("offset:"+recordMetadata.offset());
                        System.out.println("partition:"+recordMetadata.partition());
                        if (recordMetadata.hasTimestamp())
                        System.out.println("timestamp:"+recordMetadata.timestamp());
                    }
                }
            });
        }
        kafkaProducer.close();
    }
}
