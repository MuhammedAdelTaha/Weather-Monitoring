FROM maven:3.8.5-openjdk-11-slim AS build

WORKDIR /app

# Copy the pom.xml file
COPY pom.xml .

# Download all required dependencies
RUN mvn dependency:go-offline -B

# Copy the source code
COPY src ./src/

# Build the application
RUN mvn install -DskipTests

# Create runtime image
FROM openjdk:11-jre-slim

WORKDIR /app

# Copy the built jar file from the build stage
COPY --from=build /app/target/Weather-Station-1.0-SNAPSHOT.jar ./app.jar
# Copy the dependencies
COPY --from=build /app/target/lib/ ./lib/

# Set default environment variables
ENV STATION_ID=1
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# Run the weather station application
CMD ["java", "-jar", "app.jar"]