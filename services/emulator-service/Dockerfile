# Stage 1: Build
FROM gradle:8.14.0-jdk24 AS builder
WORKDIR /app
COPY services/emulator-service/gradle* ./
COPY services/emulator-service/settings.gradle .
COPY services/emulator-service/build.gradle .
COPY services/emulator-service/gradle.properties .
COPY services/emulator-service/src ./src
RUN gradle clean build -x test --no-daemon

# Stage 2: Runtime
FROM eclipse-temurin:24-jre-alpine
WORKDIR /app
COPY --from=builder /app/build/libs/emulator-service-*.jar app.jar
EXPOSE 8085
ENTRYPOINT ["java", "-jar", "app.jar"]