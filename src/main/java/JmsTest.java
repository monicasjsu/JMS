import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;

public class JmsTest {

  private static String ACTIVE_MQ_URL = "tcp://localhost:61616";
  // Create a ConnectionFactory
  private static ActiveMQConnectionFactory CONNECTION_FACTORY = new ActiveMQConnectionFactory(ACTIVE_MQ_URL);
  private static String TOPIC_NAME = "TestJMS";

  public static void main(String[] args) throws Exception {
    createProducers(3, false);
    createConsumers(4, false);
    Thread.sleep(1000);
    createProducers(5, false);
    createConsumers(2, false);
    createProducers(1, false);
    createConsumers(3, false);
    Thread.sleep(1000);
    createProducers(1, false);
    createConsumers(3, false);
    createProducers(4, false);
    createConsumers(2, false);
    Thread.sleep(1000);
    createProducers(2, false);
    createConsumers(1, false);
    createProducers(7, false);
    createConsumers(3, false);
  }

  private static void createProducers(int numProducers, boolean isDaemon) {
    for (int i=0; i < numProducers; i++) {
      spawnThread(new ActiveMqProducer(), isDaemon);
    }
  }

  private static void createConsumers(int numConsumers, boolean isDaemon) {
    for (int i=0; i < numConsumers; i++) {
      spawnThread(new ActiveMqConsumer(), isDaemon);
    }
  }

  public static void spawnThread(Runnable runnable, boolean daemon) {
    Thread brokerThread = new Thread(runnable);
    brokerThread.setDaemon(daemon);
    brokerThread.start();
  }

  public static class ActiveMqProducer implements Runnable {
    public void run() {
      try {
        // Create a Connection ans start a new session
        final Connection connection = CONNECTION_FACTORY.createConnection();
        connection.start();
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create a new topic
        final Destination destination = session.createQueue(TOPIC_NAME);

        // Create a MessageProducer from the Session to the Topic or Queue
        final  MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        // Create a messages
        final String text = "Hello world! From: " + Thread.currentThread().getName() + " : " + this.hashCode();
        TextMessage message = session.createTextMessage(text);

        // Tell the producer to send the message
        System.out.println("Sent message: "+ message.hashCode() + " : " + Thread.currentThread().getName());
        producer.send(message);

        // Clean up
        session.close();
        connection.close();
      }
      catch (Exception e) {
        System.out.println("Exception: " + e);
        e.printStackTrace();
      }
    }
  }

  public static class ActiveMqConsumer implements Runnable, ExceptionListener {

    public void run() {
      try {
        // Create a Connection ans start a new session
        final Connection connection = CONNECTION_FACTORY.createConnection();
        connection.start();
        connection.setExceptionListener(this);
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Setup destination and create a consumer
        final  Destination destination = session.createQueue(TOPIC_NAME);
        final MessageConsumer consumer = session.createConsumer(destination);

        // Wait for a message
        final Message message = consumer.receive(1000);

        if (message instanceof TextMessage) {
          final TextMessage textMessage = (TextMessage) message;
          final String text = textMessage.getText();
          System.out.println("Received: " + text);
        } else {
          System.out.println("Received: " + message);
        }

        consumer.close();
        session.close();
        connection.close();
      } catch (Exception e) {
        System.out.println("Exception: " + e);
        e.printStackTrace();
      }
    }

    public synchronized void onException(JMSException ex) {
      System.out.println("JMS Exception occurred.  Shutting down client.");
    }
  }
}
