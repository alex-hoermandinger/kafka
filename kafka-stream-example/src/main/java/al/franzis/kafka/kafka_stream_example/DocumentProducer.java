//
/////////////////////////////////////////////////////////////////
//                 C O P Y R I G H T  (c) 2017                 //
//    A G F A   H E A L T H C A R E   C O R P O R A T I O N    //
//                    All Rights Reserved                      //
/////////////////////////////////////////////////////////////////
//
//        THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
//                      AGFA CORPORATION
//       The copyright notice above does not evidence any
//      actual or intended publication of such source code.
//
/////////////////////////////////////////////////////////////////
//

package al.franzis.kafka.kafka_stream_example;

import static java.lang.String.format;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author axjrd
 *
 */
public class DocumentProducer
{
    private final Producer<String, String> producer;
    private final String topic;
    
    public DocumentProducer( Properties kafkaProps, String topic )
    {
        producer = new KafkaProducer<>(kafkaProps);
        this.topic = topic;
    }
   
    public void produce( int nrDocuments )
    {
        long start = System.currentTimeMillis();
        for ( int i = 0; i < nrDocuments; i++ )
        {
            String key = getRepositoryID();
            String value = getStorageSystemGroup() + " " + getByteSize();
            producer.send( new ProducerRecord<String, String>( topic, key, value ) );
        }
        long duration = System.currentTimeMillis() - start;
        
        System.out.println( format( "Took %d ms to produce %d documents", duration, nrDocuments ) );
    }
    
    public void close()
    {
        producer.close();
    }
    
    private String getRepositoryID()
    {
        return "Repo1";
    }
    
    private String getStorageSystemGroup()
    {
        return "SSG1";
    }
    
    private long getByteSize()
    {
        return 500;
    }
    
    
    
    
    
}
