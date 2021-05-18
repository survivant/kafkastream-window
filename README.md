il y a des bonnes explications et demo des streams et ksqldb
https://ksqldb.io/slides/kafka-summit-europe-2021/slides.html


Project package structure description : 

    config:
        KafkaProducerConfig : producer configuration
        KafkaStreamProcessorConfig : stream configuration
        SwaggerConfig : Swagger configuration
        TopicConfig : topic configuration
    
    consumer:
    
    controller:
        KafkaAdminController : endpoints for KafkaAdminClient
        ***KafkaPOCController : MAIN REST CONTROLLER
    
    manager:
    
    model:
        Order : POJO
        Return: POJO
    
    producer:
        OrderProducer : KafkaTemplate produce Order to topics : order.topic.name AND order.window.topic.name
    
    stream:
        KafkaStreamOrderProcessorWindow : @Qualifier("OrderStreamProcessorWindow") StreamsBuilder : Input topic : order.window.topic.name
            Read from topic
            groupby : order.getStatus()
            .windowedBy(TimeWindows.of(Duration.ofMinutes(1L)))
            .count(Materialized.as(countsWindow));
            output stream : order.stream.window.output.name

    KafkaEventAlarmApplication : Main 


    Docker Compose for Kafka :  src/test/resources/docker-compose.yml

    

To launch Kafka : go into "src/test/resources/docker-compose.yml"

docker-compose up -V


Access Swagger-UI

TO CREATE ORDER : 

    http://localhost:8282/swagger-ui.html#/kafka-poc-controller/sendMessageToKafkaTopicUsingPOST

or 

    curl -X POST "http://localhost:8282/kafka/createOrder" -H "accept: */*" -H "Content-Type: application/json" -d "{ \"orderId\": \"string\", \"orderTimestamp\": \"string\", \"product\": \"XYZ\", \"status\": \"NEW\"}"


TO GET ORDER COUNT

    http://localhost:8282/swagger-ui.html#/kafka-poc-controller/getInteractiveQueryCountUsingGET

or

    curl -X GET "http://localhost:8282/kafka/getInteractiveQueryCount" -H "accept: */*"


TO GET ORDER COUNT WITH WINDOW

    http://localhost:8282/swagger-ui.html#/kafka-poc-controller/getInteractiveQueryCountLastMinuteUsingGET

or

    curl -X GET "http://localhost:8282/kafka/getInteractiveQueryCountLastMinute" -H "accept: */*"



ERRORS : 

    org.apache.kafka.streams.errors.InvalidStateStoreException: The state store, orders-status-count, may have migrated to another instance.
    at org.apache.kafka.streams.state.internals.QueryableStoreProvider.getStore(QueryableStoreProvider.java:76) ~[kafka-streams-2.6.0.jar:na]
    at org.apache.kafka.streams.KafkaStreams.store(KafkaStreams.java:1208) ~[kafka-streams-2.6.0.jar:na]
    at org.apache.kafka.streams.KafkaStreams.store(KafkaStreams.java:1194) ~[kafka-streams-2.6.0.jar:na]
    at com.example.kafkaeventalarm.stream.KafkaStreamOrderProcessor.getInteractiveQueryCount(KafkaStreamOrderProcessor.java:83) ~[classes/:na]
    at com.example.kafkaeventalarm.controller.KafkaPOCController.getInteractiveQueryCount(KafkaPOCController.java:75) ~[classes/:na]
    at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:na]
    at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62) ~[na:na]
    at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:na]
    at java.base/java.lang.reflect.Method.invoke(Method.java:566) ~[na:na]
    at org.springframework.web.method.support.InvocableHandlerMethod.doInvoke(InvocableHandlerMethod.java:197) ~[spring-web-5.3.3.jar:5.3.3]
    at org.springframework.web.method.support.InvocableHandlerMethod.invokeForRequest(InvocableHandlerMethod.java:141) ~[spring-web-5.3.3.jar:5.3.3]
    at org.springframework.web.servlet.mvc.method.annotation.ServletInvocableHandlerMethod.invokeAndHandle(ServletInvocableHandlerMethod.java:106) ~[spring-webmvc-5.3.3.jar:5.3.3]
    at org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter.invokeHandlerMethod(RequestMappingHandlerAdapter.java:894) ~[spring-webmvc-5.3.3.jar:5.3.3]
    at org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter.handleInternal(RequestMappingHandlerAdapter.java:808) ~[spring-webmvc-5.3.3.jar:5.3.3]
    at org.springframework.web.servlet.mvc.method.AbstractHandlerMethodAdapter.handle(AbstractHandlerMethodAdapter.java:87) ~[spring-webmvc-5.3.3.jar:5.3.3]
    at org.springframework.web.servlet.DispatcherServlet.doDispatch(DispatcherServlet.java:1060) ~[spring-webmvc-5.3.3.jar:5.3.3]
    at org.springframework.web.servlet.DispatcherServlet.doService(DispatcherServlet.java:962) ~[spring-webmvc-5.3.3.jar:5.3.3]
    at org.springframework.web.servlet.FrameworkServlet.processRequest(FrameworkServlet.java:1006) ~[spring-webmvc-5.3.3.jar:5.3.3]
    at org.springframework.web.servlet.FrameworkServlet.doGet(FrameworkServlet.java:898) ~[spring-webmvc-5.3.3.jar:5.3.3]
    at javax.servlet.http.HttpServlet.service(HttpServlet.java:626) ~[tomcat-embed-core-9.0.41.jar:4.0.FR]
    at org.springframework.web.servlet.FrameworkServlet.service(FrameworkServlet.java:883) ~[spring-webmvc-5.3.3.jar:5.3.3]
    at javax.servlet.http.HttpServlet.service(HttpServlet.java:733) ~[tomcat-embed-core-9.0.41.jar:4.0.FR]
    at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:231) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:166) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:53) ~[tomcat-embed-websocket-9.0.41.jar:9.0.41]
    at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:193) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:166) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.springframework.web.filter.RequestContextFilter.doFilterInternal(RequestContextFilter.java:100) ~[spring-web-5.3.3.jar:5.3.3]
    at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:119) ~[spring-web-5.3.3.jar:5.3.3]
    at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:193) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:166) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.springframework.web.filter.FormContentFilter.doFilterInternal(FormContentFilter.java:93) ~[spring-web-5.3.3.jar:5.3.3]
    at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:119) ~[spring-web-5.3.3.jar:5.3.3]
    at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:193) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:166) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:201) ~[spring-web-5.3.3.jar:5.3.3]
    at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:119) ~[spring-web-5.3.3.jar:5.3.3]
    at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:193) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:166) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:202) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:97) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:542) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:143) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:92) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:78) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:343) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.apache.coyote.http11.Http11Processor.service(Http11Processor.java:374) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.apache.coyote.AbstractProcessorLight.process(AbstractProcessorLight.java:65) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.apache.coyote.AbstractProtocol$ConnectionHandler.process(AbstractProtocol.java:888) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1597) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at org.apache.tomcat.util.net.SocketProcessorBase.run(SocketProcessorBase.java:49) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128) ~[na:na]
    at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628) ~[na:na]
    at org.apache.tomcat.util.threads.TaskThread$WrappingRunnable.run(TaskThread.java:61) ~[tomcat-embed-core-9.0.41.jar:9.0.41]
    at java.base/java.lang.Thread.run(Thread.java:834) ~[na:na]
