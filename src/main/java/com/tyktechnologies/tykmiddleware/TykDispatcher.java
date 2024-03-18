package com.tyktechnologies.tykmiddleware;

import coprocess.DispatcherGrpc;
import coprocess.CoprocessObject;
import coprocess.CoprocessSessionState;
import coprocess.CoprocessReturnOverrides;

import org.apache.activemq.ActiveMQConnectionFactory;

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

public class TykDispatcher extends DispatcherGrpc.DispatcherImplBase {

    @Override
    public void dispatch(CoprocessObject.Object request,
            io.grpc.stub.StreamObserver<CoprocessObject.Object> responseObserver) {

        CoprocessObject.Object modifiedRequest = null;

        System.out.println("*** Incoming Request ***");
        System.out.println("Hook name: " + request.getHookName());

        switch (request.getHookName()) {
            case "JMSPostMiddleware":
                modifiedRequest = JMSPostMiddleware(request);
            default:
                // Do nothing, the hook name isn't implemented!
        }

        // Return the modified request (if the transformation was done):
        if (modifiedRequest != null) {
            responseObserver.onNext(modifiedRequest);
        }

        responseObserver.onCompleted();

    }

    public static class HelloWorldProducer implements Runnable {
        public void run() {
            try {
                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();

                // Create a Session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                // Create the destination (Topic or Queue)
                Destination destination = session.createQueue("test.foo");

                // Create a MessageProducer from the Session to the Topic or Queue
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                // Create a messages
                String text = "Hello world";
                TextMessage message = session.createTextMessage(text);

                // Tell the producer to send the message
                producer.send(message);

                // Clean up
                session.close();
                connection.close();
            }
            catch (Exception e) {
                System.out.println("Caught exception: " + e);
                e.printStackTrace();
            }
        }
    }

    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }

    CoprocessObject.Object JMSPostMiddleware(CoprocessObject.Object request) {

        CoprocessObject.Object.Builder builder = request.toBuilder();
        thread(new HelloWorldProducer(), false);
        return builder.build();
    }

}
