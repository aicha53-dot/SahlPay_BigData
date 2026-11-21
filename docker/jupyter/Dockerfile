FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu

USER root

RUN python3 -m pip install --no-cache-dir \
    jupyterlab==4.4.9 \
    pyspark==3.5.7

RUN mkdir -p /workspace /home/spark && \
    usermod -d /home/spark spark && \
    chown -R spark:spark /workspace /home/spark

ENV HOME=/home/spark
ENV JUPYTER_CONFIG_DIR=/home/spark/.jupyter
ENV JUPYTER_DATA_DIR=/home/spark/.local/share/jupyter
ENV JUPYTER_RUNTIME_DIR=/home/spark/.local/share/jupyter/runtime

USER spark

WORKDIR /workspace