# Analytics workbench: Apache Spark 3.5.3 (Java 11, Python 3.8) + every Python library the project
# uses (pymongo, happybase, faker, pandas, matplotlib, numpy) + the MongoDB Spark Connector.
#
# Everything is installed OFFLINE from files committed in the repo:
#   ./wheels  - Linux/cp38 wheels downloaded on the host (`pip download --platform manylinux2014_x86_64
#               --python-version 3.8 --only-binary=:all: ...`), because the build network sits behind a
#               TLS-intercepting proxy that containers do not trust.
#   ./wheels/vendor/thriftpy2 - pure-Python source of thriftpy2 (happybase's Thrift library has no cp38
#               Linux wheel); the Cython switch is turned off so it uses its pure-Python transports.
#   ./jars    - mongo-spark-connector_2.12-10.4.0 + mongodb-driver-sync/core, bson 5.1.4. They must be
#               on the classpath at JVM launch because `spark-submit` starts the JVM before user code.
FROM spark:3.5.3-python3

USER root
COPY wheels /tmp/wheels
RUN pip3 install --no-cache-dir --no-index --no-deps /tmp/wheels/*.whl \
 && cp -r /tmp/wheels/vendor/thriftpy2 /usr/local/lib/python3.8/dist-packages/ \
 && sed -i 's/^CYTHON = not PYPY.*/CYTHON = False  # pure-Python build (no compiled extensions)/' \
        /usr/local/lib/python3.8/dist-packages/thriftpy2/_compat.py \
 && python3 -c "import pymongo, happybase, faker, pandas, matplotlib, numpy; print('python libs OK')" \
 && rm -rf /tmp/wheels

COPY jars/*.jar /opt/spark/jars/
RUN mkdir -p /opt/spark/ivy && chmod -R 777 /opt/spark/ivy

ENV PATH="/opt/spark/bin:${PATH}" \
    PYSPARK_PYTHON=python3 \
    PYTHONUNBUFFERED=1 \
    MPLCONFIGDIR=/tmp/matplotlib

WORKDIR /project
CMD ["sleep", "infinity"]
