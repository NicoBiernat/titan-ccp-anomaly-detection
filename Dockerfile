FROM openjdk:11-slim

ADD build/distributions/titanccp-anomalydetection.tar /

CMD JAVA_OPTS="$JAVA_OPTS -Dorg.slf4j.simpleLogger.defaultLogLevel=$LOG_LEVEL" \
    /titanccp-anomalydetection/bin/titanccp-anomalydetection
