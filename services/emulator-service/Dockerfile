FROM gradle:8.14.0-jdk24 AS builder
WORKDIR /app
COPY . ./
RUN gradle clean build -x test --no-daemon

FROM eclipse-temurin:24-jdk-alpine
WORKDIR /app
COPY --from=builder /app/build/libs/*.jar app.jar
EXPOSE 8085
ENTRYPOINT ["java", "-jar", "app.jar"]