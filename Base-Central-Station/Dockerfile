FROM maven:3.8.5-openjdk-17-slim AS build

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
FROM maven:3.8.5-openjdk-17-slim

WORKDIR /app

# Copy the built jar file from the build stage
COPY --from=build /app/target/Base-Central-Station-1.0-SNAPSHOT-jar-with-dependencies.jar ./app.jar

# Run the weather station application
CMD ["java", "-jar", "app.jar"]