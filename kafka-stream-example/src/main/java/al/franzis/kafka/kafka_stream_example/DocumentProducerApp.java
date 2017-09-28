package al.franzis.kafka.kafka_stream_example;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;


public class DocumentProducerApp {

    public static void main( String[] args )
    {
        Properties props = new Properties();
        props.put( "bootstrap.servers", "localhost:9092" );
        props.put( "acks", "all" );
        props.put( "retries", 0 );
        props.put( "batch.size", 16384 );
        props.put( "linger.ms", 1 );
        props.put( "buffer.memory", 33554432 );
        props.put( "key.serializer", "org.apache.kafka.common.serialization.StringSerializer" );
        props.put( "value.serializer", "org.apache.kafka.common.serialization.StringSerializer" );
        
        DocumentProducer producer = null;
        BufferedReader br = null;
        try
        {
            producer = new DocumentProducer( props, Constants.REPOSITORY_DOCUMENT_STORE_TOPIC );
            
            br = new BufferedReader( new InputStreamReader( System.in ) );
            
            while ( true )
            {
                
                System.out.print( "Enter number of documents to produce (or 'q' to exit): " );
                String input = br.readLine();
                
                if ( "q".equals( input ) )
                {
                    System.out.println( "Exit!" );
                    System.exit( 0 );
                }
                
                int nrDocuments = Integer.parseInt( input );
                producer.produce( nrDocuments );
            }
            
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
        finally
        {
            if ( producer != null )
            {
                producer.close();
            }
        }
        
    }
}
