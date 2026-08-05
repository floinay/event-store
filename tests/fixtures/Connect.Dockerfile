FROM quay.io/debezium/connect:3.6.0.Final

USER root
RUN microdnf install -y iproute-tc && microdnf clean all
USER kafka
