FROM openjdk:8-jre-alpine

ENV JAVA_HEAP=4g

ADD /target/distributed-join-*-standalone.jar /opt/distributed-join/
ADD /version /opt/distributed-join/version
ADD /config/prod /opt/distributed-join/config/prod

RUN ln -sf /opt/distributed-join/distributed-join-*-standalone.jar /opt/distributed-join/distributed-join.jar

WORKDIR /opt/distributed-join
ENTRYPOINT exec java "-Xms${JAVA_HEAP}" "-Xmx${JAVA_HEAP}" "-Dsun.net.inetaddr.ttl=30" "-Dsun.net.inetaddr.negative.ttl=2" "-XX:+UseG1GC" -jar /opt/distributed-join/distributed-join.jar

EXPOSE 3000
