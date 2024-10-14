FROM openjdk:21

WORKDIR /app

COPY passage.jar /app/passage.jar

CMD ["java", "-Xmx52g", "-jar", "/app/passage.jar", "--database=/blazegraph.jnl"]