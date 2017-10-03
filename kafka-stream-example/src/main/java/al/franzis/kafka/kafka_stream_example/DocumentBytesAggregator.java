package al.franzis.kafka.kafka_stream_example;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

/**
 * 
 * @author alexander.hoermandinger
 */
public class DocumentBytesAggregator {
	private static final long WINDOW_SIZE_MS = 120_000;
	public static final SimpleDateFormat dateFormat = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" );
	
	private final Properties props;
	private KafkaStreams streams;
	
	
	public DocumentBytesAggregator( Properties props )
	{
	    this.props = props;
	}
	
    public void start()
    {
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
                "ssg2AggregatedBytesTable" );
        
        
        ssg2AggregatedBytesTable.toStream( 
        		(wk, v) -> {
        			String start = dateFormat.format(new Date( wk.window().start() ) );
        			String end = dateFormat.format(new Date( wk.window().end() ) );
        			return wk.key() + "[" + start + "-" + end + "]" ;
        		} )
        	.to( Constants.REPOSITORY_BYTES_STORE_TOPIC );
        
        streams = new KafkaStreams( builder, props );
        streams.start();
    }
    
    public KeyValueIterator<Long, Long> query( String ssgID )
    {
        ReadOnlyWindowStore<String, Long> windowStore =
                streams.store("ssg2AggregatedBytesTable", QueryableStoreTypes.windowStore());
        long timeFrom = 0; // beginning of time = oldest available
        long timeTo = System.currentTimeMillis(); // now (in processing-time)
        WindowStoreIterator<Long> iterator = windowStore.fetch( ssgID, timeFrom, timeTo);
        return iterator;
    }
    
    public void close()
    {
        streams.close();
    }
    
}
