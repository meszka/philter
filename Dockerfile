FROM eclipse-temurin:21

WORKDIR /app

COPY target/scala-2.13/philter-assembly-0.1.0-SNAPSHOT.jar app.jar

CMD ["java", "-jar", "app.jar"]
