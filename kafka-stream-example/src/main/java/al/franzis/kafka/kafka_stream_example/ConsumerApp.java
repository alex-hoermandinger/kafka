package al.franzis.kafka.kafka_stream_example;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;


public class ConsumerApp {

	
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test3");
		props.put("enable.auto.commit", "false");
		//props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
//		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(Constants.STRING_FILE_INPUT_TOPIC), new RebalanceListener(consumer));
		
		//Set<TopicPartition> assignedPartitions = consumer.assignment();
		
		
		//for(TopicPartition partition : assignedPartitions) {
//			System.out.println(partition);
		//}
		
		SimpleDateFormat dateFormat = new SimpleDateFormat();
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records)
			{
				Date tsDate = new Date( record.timestamp() );
				System.out.printf("offset = %d, timestamp= %s, key = %s, value = %s%n", record.offset(), dateFormat.format(tsDate), record.key(), record.value());
			}
		}
	}
	
	private static class RebalanceListener implements ConsumerRebalanceListener {

		private final KafkaConsumer<?,?> consumer;
		
		private RebalanceListener(KafkaConsumer<?,?> consumer) {
			this.consumer = consumer;
		}
		
		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			System.out.println("Revoked partitions: " + partitions);
			
		}

		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			System.out.println("Assigned partitions: " + partitions);
			Map<TopicPartition,Long> beginningOffsets = consumer.beginningOffsets(partitions);
			System.out.println("beginningOffsets: \n" + beginningOffsets);
			
			Map<TopicPartition,Long> endOffsets = consumer.endOffsets(partitions);
			System.out.println("endOffsets: \n" + endOffsets);
//			consumer.seekToBeginning(partitions);
			
			for(TopicPartition partition : partitions) {
				System.out.println("Partition " + partition.topic() + " current position: " + consumer.position(partition));
				
			}
			
			
		}
		
	}
}
