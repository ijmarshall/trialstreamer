ARG OSVER=ubuntu:16.04
FROM $OSVER

ENV DEBIAN_FRONTEND noninteractive

# create deploy user
RUN useradd --create-home --home /var/lib/deploy deploy

# install apt-get requirements
ADD apt-requirements.txt /tmp/apt-requirements.txt
RUN apt-get -qq update -y
RUN xargs -a /tmp/apt-requirements.txt apt-get install -y --no-install-recommends && apt-get clean && rm -rf /etc/cron.*/*

# Certs
RUN mkdir -p /etc/pki/tls/certs && \
    ln -s /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt

RUN chown -R deploy.deploy /var/lib/deploy/

USER deploy
# install Anaconda
RUN aria2c -s 16 -x 16 -k 30M https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /var/lib/deploy/Anaconda.sh
RUN cd /var/lib/deploy && bash Anaconda.sh -b && rm -rf Anaconda.sh
ENV PATH=/var/lib/deploy/miniconda3/bin:$PATH
ADD trialstreamer_env.yml tmp/trialstreamer_env.yml
RUN conda env create -f tmp/trialstreamer_env.yml
# from https://stackoverflow.com/questions/37945759/condas-source-activate-virtualenv-does-not-work-within-dockerfile
ENV PATH /var/lib/deploy/miniconda3/envs/trialstreamer/bin:$PATH


USER root
ADD server.py /var/lib/deploy/
ADD entrypoint.sh /var/lib/deploy/
ADD update.py /var/lib/deploy/
ADD crontab /etc/cron.d/crontab
ADD trialstreamer /var/lib/deploy/trialstreamer
RUN mkdir -p /var/lib/deploy/pubmed-data
RUN chown -R deploy.deploy /var/lib/deploy/trialstreamer

ENV HOME /var/lib/deploy

WORKDIR /var/lib/deploy

USER root

RUN pip install gunicorn gevent

ENTRYPOINT ["/var/lib/deploy/entrypoint.sh"]
