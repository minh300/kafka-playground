package minh.event;


import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.clients.producer.MockProducer;
import org.junit.jupiter.api.Test;

import com.launchdarkly.eventsource.MessageEvent;

import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaEventHandlerTest {
	
	@Test
	public void onMessageTest() {
		MockProducer<String, String> mockProducer = new MockProducer<String, String>(true, new StringSerializer(),
				new StringSerializer());
		KafkaEventHandler evenHandler = new KafkaEventHandler(mockProducer, "someTopic");
		evenHandler.onMessage(null, new MessageEvent("This is a message"));
		evenHandler.onMessage(null, new MessageEvent("This is a message 2"));
	    assertTrue(mockProducer.history().size() == 2);
	}
}
