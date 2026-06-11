# Use Maven image to build the application
FROM maven:3.9.6-eclipse-temurin-21 AS builder

# Set working directory
WORKDIR /app

# Copy pom.xml and Maven files
COPY pom.xml .
COPY .mvn .mvn

# Copy source code
COPY src src

# Build the application JAR
RUN mvn clean package -DskipTests

# Use a minimal JDK image for running
FROM eclipse-temurin:21-jre

# Set working directory
WORKDIR /app

# Create a user to run the application (non-root)
RUN adduser --disabled-password --gecos '' appuser && \
    chown -R appuser:appuser /app
USER appuser

# Copy the built JAR from the builder stage
COPY --from=builder /app/target/*.jar app.jar

# Expose the port
EXPOSE 8080

# Run the application
ENTRYPOINT ["java", "-jar", "app.jar"]
