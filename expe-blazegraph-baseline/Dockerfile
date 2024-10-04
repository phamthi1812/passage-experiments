FROM openjdk:21

WORKDIR /app

COPY embedded-blazegraph-jar-with-dependencies.jar /app/blazegraph.jar

CMD ["java", "-Xmx52g", "-jar", "/app/blazegraph.jar", "--database=/blazegraph.jnl"]