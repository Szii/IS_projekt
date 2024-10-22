# Use an official Maven image with JDK 18 for building the project
FROM maven:3.8.6-eclipse-temurin-18 AS build

# Set the working directory in the container
WORKDIR /app

# Copy the Maven configuration and source code to the container
COPY pom.xml /app/pom.xml
COPY src /app/src

# Build the project using Maven, skipping tests if desired
RUN mvn clean package -DskipTests

# Use an official OpenJDK image with JDK 18 for running the application
FROM openjdk:18-jdk-alpine

# Set the working directory in the container
WORKDIR /app

# Copy the built Spring Boot JAR file from the build stage
# This assumes the JAR file name is known or consistent
COPY --from=build /app/target/*.jar MongoFetchInsert-1.0-SNAPSHOT.jar

# Expose the port your Spring Boot application runs on (9090)
EXPOSE 9090

# Run the Spring Boot application
ENTRYPOINT ["java", "-jar", "/app/MongoFetchInsert-1.0-SNAPSHOT.jar"]