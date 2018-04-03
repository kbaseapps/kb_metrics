FROM kbase/kbase:sdkbase.latest
MAINTAINER KBase Developer
# -----------------------------------------
# In this section, you can install any system dependencies required
# to run your App.  For instance, you could place an apt-get update or
# install line here, a git checkout to download code, or run any other
# installation scripts.

# Here we install a python coverage tool and an
# https library that is out of date in the base image.

RUN pip install coverage

# update security libraries in the base image
#RUN pip install cffi --upgrade \
#    && pip install pyopenssl --upgrade \
#    && pip install ndg-httpsclient --upgrade \
#    && pip install pyasn1 --upgrade \
#    && pip install requests --upgrade \
#    && pip install 'requests[security]' --upgrade

# Update certs
RUN apt-get update
RUN apt-get install ca-certificates

# install mongodb
RUN sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10 \
    && echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | tee /etc/apt/sources.list.d/mongodb.list  \
    && sudo apt-get update \
    && sudo apt-get install -y mongodb \
    && sudo apt-get install -y mongodb-org=2.6.2 mongodb-org-server=2.6.2 mongodb-org-shell=2.6.2 mongodb-org-mongos=2.6.2 mongodb-org-tools=2.6.2


# Fix Python SSL warnings for python < 2.7.9 (system python on Trusty is 2.7.6)
# https://github.com/pypa/pip/issues/4098
RUN pip install pip==8.1.2
RUN pip install --disable-pip-version-check requests requests_toolbelt pyopenssl --upgrade

#RUN pip easy_install -U pymongo
RUN pip install pymongo --upgrade
# -----------------------------------------

COPY ./ /kb/module
RUN mkdir -p /kb/module/work
RUN chmod -R a+rw /kb/module

WORKDIR /kb/module

RUN make all

ENTRYPOINT [ "./scripts/entrypoint.sh" ]

CMD [ ]
