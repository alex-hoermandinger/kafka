package al.franzis.kafka.kafka_stream_example;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;

/**
 * Hello world!
 *
 */
public class DocumentBytesAggregator {
	
    public static void main( String[] args )
    {
        Properties props = new Properties();
        props.put( StreamsConfig.APPLICATION_ID_CONFIG, "document-bytes-aggregator" );
        props.put( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092" );
        props.put( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName() );
        props.put( StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName() );
       
        
        // setting offset reset to earliest so that we can re-run the demo code
        // with the same pre-loaded data
        props.put( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest" );
        
        KStreamBuilder builder = new KStreamBuilder();
        
        KStream<String, String> sourceStream = builder.stream( Serdes.String(), Serdes.String(), Constants.REPOSITORY_DOCUMENT_STORE_TOPIC );
        
        KStream<String, Long> ssg2BytesStream = 
                sourceStream.map(new KeyValueMapper<String, String, KeyValue<String, Long>>()
        {
            @Override
            public KeyValue<String, Long> apply( String key, String value )
            {
                String[] valueComponents = value.split( " " );
                String storageSystemGroupID = valueComponents[0];
                long bytes = Long.parseLong( valueComponents[1] );
                return new KeyValue<>( storageSystemGroupID, bytes );
            }
        } );
        
        KTable<String,Long> ssg2AggregatedBytesTable = ssg2BytesStream.groupByKey().aggregate( 
                () -> 0L, 
                (aggKey, aggValue, aggregate) ->(Long)(aggregate + aggValue),
             //   TimeWindows.of( 3000 ),
                Serdes.Long() );
       
        // need to override value serde to Long type
        ssg2AggregatedBytesTable.to( Serdes.String(), Serdes.Long(), Constants.REPOSITORY_BYTES_STORE_TOPIC );
        
        KafkaStreams streams = new KafkaStreams( builder, props );
        streams.start();
        
        try (BufferedReader br = new BufferedReader( new InputStreamReader( System.in ) ))
        {
            while ( true )
            {
                System.out.print( "Enter 'q' to exit): " );
                String input = br.readLine();
                
                if ( "q".equals( input ) )
                {
                    System.out.println( "Exit!" );
                    System.exit( 0 );
                }
            }
            
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
        
        streams.close();
    }
}