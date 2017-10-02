package al.franzis.kafka.kafka_stream_example;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
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
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

/**
 * Hello world!
 *
 */
public class DocumentBytesAggregator {
	private static final long WINDOW_SIZE_MS = 120_000;
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" );
	
	
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
        
        KTable<Windowed<String>,Long> ssg2AggregatedBytesTable = ssg2BytesStream.groupByKey().aggregate( 
                () -> 0L, 
                (aggKey, aggValue, aggregate) -> aggregate + aggValue,
                TimeWindows.of( WINDOW_SIZE_MS ),
                Serdes.Long(),
                "ssg2AggregatedBytesTable");
        
        
        ssg2AggregatedBytesTable.toStream( 
        		(wk, v) -> {
        			String start = dateFormat.format(new Date( wk.window().start() ) );
        			String end = dateFormat.format(new Date( wk.window().end() ) );
        			return wk.key() + "[" + start + "-" + end + "]" ;
        		} )
        	.to( Constants.REPOSITORY_BYTES_STORE_TOPIC );
        
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
                    streams.close();
                    System.exit( 0 );
                } else if (input.startsWith("p") )
                {
                	String ssgID = input.split(" ")[1];
                	
                	ReadOnlyWindowStore<String, Long> windowStore =
                		    streams.store("ssg2AggregatedBytesTable", QueryableStoreTypes.windowStore());
                	long timeFrom = 0; // beginning of time = oldest available
                	long timeTo = System.currentTimeMillis(); // now (in processing-time)
                	WindowStoreIterator<Long> iterator = windowStore.fetch( ssgID, timeFrom, timeTo);
                	while (iterator.hasNext()) {
                	  KeyValue<Long, Long> next = iterator.next();
                	  long windowTimestamp = next.key;
                	  String start = dateFormat.format(new Date( windowTimestamp ) );
                	  System.out.println("Aggregated Bytes stored for SSG '" + ssgID + "': window-start: " + start + ", bytes: " + next.value);
                	}
                }
            }
            
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }
}
