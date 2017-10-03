package al.franzis.kafka.kafka_stream_example;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.KeyValueIterator;

/**
 * 
 * @author alexander.hoermandinger
 */
public class DocumentBytesAggregatorApp 
{
	
    public static void main( String[] args )
    {
        Properties props = new Properties();
        props.put( StreamsConfig.APPLICATION_ID_CONFIG, "document-bytes-aggregator" );
        props.put( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092" );
        props.put( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName() );
        props.put( StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName() );
       
        DocumentBytesAggregator bytesAggregator = new DocumentBytesAggregator( props );
        bytesAggregator.start();
        
        try (BufferedReader br = new BufferedReader( new InputStreamReader( System.in ) ))
        {
            while ( true )
            {
                System.out.print( "Enter 'q' to exit): " );
                String input = br.readLine();
                
                if ( "q".equals( input ) )
                {
                    System.out.println( "Exit!" );
                    bytesAggregator.close();
                    System.exit( 0 );
                } else if (input.startsWith("p") )
                {
                	String ssgID = input.split(" ")[1];
                	
                	KeyValueIterator<Long,Long> iterator = bytesAggregator.query( ssgID );
                	
                	while (iterator.hasNext()) {
                	  KeyValue<Long, Long> next = iterator.next();
                	  long windowTimestamp = next.key;
                	  String start = DocumentBytesAggregator.dateFormat.format(new Date( windowTimestamp ) );
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
