package hello;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.cloud.gcp.pubsub.integration.AckMode;
import org.springframework.cloud.gcp.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import org.springframework.cloud.gcp.pubsub.integration.outbound.PubSubMessageHandler;
import org.springframework.cloud.gcp.pubsub.support.GcpPubSubHeaders;
import org.springframework.cloud.gcp.pubsub.support.BasicAcknowledgeablePubsubMessage;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

import com.google.cloud.pubsub.v1.AckReplyConsumer;

@SpringBootApplication
public class PubSubApplication {

  private static final Log LOGGER = LogFactory.getLog(PubSubApplication.class);

  public static void main(String[] args) throws IOException {
	SpringApplication.run(PubSubApplication.class, args);
  }

  // Inbound channel adapter.

  // tag::pubsubInputChannel[]
  @Bean
  public MessageChannel pubsubInputChannel() {
	return new DirectChannel();
  }
  // end::pubsubInputChannel[]

  // tag::messageChannelAdapter[]
  @Bean
  public PubSubInboundChannelAdapter messageChannelAdapter(
	  @Qualifier("pubsubInputChannel") MessageChannel inputChannel,
	  PubSubTemplate pubSubTemplate) {
	PubSubInboundChannelAdapter adapter =
		new PubSubInboundChannelAdapter(pubSubTemplate, "testSubscription");
	adapter.setOutputChannel(inputChannel);
	adapter.setAckMode(AckMode.MANUAL);

	return adapter;
  }
  // end::messageChannelAdapter[]

  // tag::messageReceiver[]
  @Bean
  @ServiceActivator(inputChannel = "pubsubInputChannel")
  public MessageHandler messageReceiver() {
	return message -> {
	  LOGGER.info("Message arrived! Payload: " + new String((byte[]) message.getPayload()));
	  BasicAcknowledgeablePubsubMessage originalMessage =
		message.getHeaders().get(GcpPubSubHeaders.ORIGINAL_MESSAGE, BasicAcknowledgeablePubsubMessage.class);
	  originalMessage.ack();
	};
  }
  // end::messageReceiver[]

  // Outbound channel adapter

  // tag::messageSender[]
  @Bean
  @ServiceActivator(inputChannel = "pubsubOutputChannel")
  public MessageHandler messageSender(PubSubTemplate pubsubTemplate) {
	return new PubSubMessageHandler(pubsubTemplate, "testTopic");
  }
  // end::messageSender[]

  // tag::messageGateway[]
  @MessagingGateway(defaultRequestChannel = "pubsubOutputChannel")
  public interface PubsubOutboundGateway {

	void sendToPubsub(String text);
  }
  // end::messageGateway[]
}
