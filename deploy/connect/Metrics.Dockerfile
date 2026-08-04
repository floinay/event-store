FROM eclipse-temurin:21-jre
ARG JMX_EXPORTER_VERSION=1.6.0
ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_standalone/${JMX_EXPORTER_VERSION}/jmx_prometheus_standalone-${JMX_EXPORTER_VERSION}.jar /opt/jmx-exporter.jar
ENTRYPOINT ["java", "-jar", "/opt/jmx-exporter.jar"]
CMD ["9404", "/etc/jmx-exporter/config.yaml"]
