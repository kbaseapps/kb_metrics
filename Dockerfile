FROM kbase/sdkbase2:python
MAINTAINER KBase Developer
# -----------------------------------------
# In this section, you can install any system dependencies required
# to run your App.  For instance, you could place an apt-get update or
# install line here, a git checkout to download code, or run any other
# installation scripts.

# Update system and certs
RUN apt-get update && \
    apt-get install -y ca-certificates wget && \
    wget -qO - https://www.mongodb.org/static/pgp/server-3.6.asc | sudo apt-key add - && \
    echo "deb [ arch=amd64 ] http://repo.mongodb.com/apt/ubuntu trusty/mongodb-enterprise/3.6 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-enterprise.list && \
    apt-get update && \
    apt-get install -y mongodb && \
    apt-get clean && \
    rm -f /var/lib/apt/lists/*_*


# install python dependencies beyond the base image
RUN pip install pymongo python-dateutil

# -----------------------------------------

COPY ./ /kb/module
RUN mkdir -p /kb/module/work
RUN chmod -R a+rw /kb/module

# TESTING use local kb_sdk
# RUN mkdir /root/src2 \
#     && cd /root/src2 \
#     && git clone -b make-python-impl-pep8-compliant https://github.com/eapearson/kb_sdk.git \
#     && cd kb_sdk \
#     && make \
#     && cp bin/kb-sdk /usr/local/bin \
#     && mkdir -p /kb/deployment/lib /kb/deployment/lib
# TESTING over

WORKDIR /kb/module

RUN make all

RUN chmod +x /kb/module/scripts/start_lazy_uwsgi_server.sh

ENTRYPOINT [ "./scripts/entrypoint.sh" ]

CMD [ ]
