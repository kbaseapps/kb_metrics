FROM kbase/sdkbase2:python
MAINTAINER KBase Developer
# -----------------------------------------
# In this section, you can install any system dependencies required
# to run your App.  For instance, you could place an apt-get update or
# install line here, a git checkout to download code, or run any other
# installation scripts.

# Update system and certs
RUN apt-get update && \
    apt-get upgrade -y  && \
    apt-get dist-upgrade -y && \
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

WORKDIR /kb/module

RUN make all

ENTRYPOINT [ "./scripts/entrypoint.sh" ]

CMD [ ]
