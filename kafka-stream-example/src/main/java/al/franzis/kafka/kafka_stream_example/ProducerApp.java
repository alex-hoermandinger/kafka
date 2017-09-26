package al.franzis.kafka.kafka_stream_example;

import static java.lang.String.format;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class ProducerApp {
	private final static int NR_RECORDS = 10_000000;
	
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		Producer<String, String> producer = new KafkaProducer<>(props);
		
		long start = System.currentTimeMillis();
		for (int i = 0; i < NR_RECORDS; i++) {
			String intString = Integer.toString(i);
			String key = "Key_" + intString;
			String value = "Value_" + intString;
			producer.send(new ProducerRecord<String, String>(Constants.STRING_FILE_INPUT_TOPIC, key, value));
		}
		long duration = System.currentTimeMillis() - start;
		
		System.out.println(format("Took %d ms to produce %d records", duration, NR_RECORDS));

		producer.close();
	}
}
