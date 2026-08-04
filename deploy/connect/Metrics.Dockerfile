FROM eclipse-temurin@sha256:273396ed5998598ed1091e8d72711c2d36980a0e65103859c55a4e977a41ffd3
ARG JMX_EXPORTER_VERSION=1.6.0
ADD --checksum=sha256:6314f19186eb97023d424f5bdf108269fd86dbb0c70f65809b4d8751d68842b9 https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_VERSION}/jmx_prometheus_standalone-${JMX_EXPORTER_VERSION}.jar /opt/jmx-exporter.jar
ENTRYPOINT ["java", "-jar", "/opt/jmx-exporter.jar"]
CMD ["9404", "/etc/jmx-exporter/config.yaml"]
