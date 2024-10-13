FROM openjdk:21

WORKDIR /app

COPY blazegraph-cli.jar /app/blazegraph-cli.jar

CMD ["java", "-Xmx52g", "-jar", "/app/blazegraph-cli.jar", "--database=/blazegraph.jnl"]