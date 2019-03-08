FROM ubuntu:18.04

RUN apt-get update && apt-get install -y \
    bc \
    curl \
    redis-server \
    vim

RUN cd /usr/local/bin \
    && curl -O https://storage.googleapis.com/kubernetes-release/release/v1.6.2/bin/linux/amd64/kubectl \
    && chmod 755 /usr/local/bin/kubectl

COPY ./autoscale.sh /bin/autoscale.sh
RUN chmod +x /bin/autoscale.sh

CMD ["bash","/bin/autoscale.sh"]
