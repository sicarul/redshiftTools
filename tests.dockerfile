FROM 996097627176.dkr.ecr.us-east-1.amazonaws.com/data-zapier-nightly:latest

ENV ENVIRONMENT=production

COPY bootstrap.R /tmp/bootstrap.R
COPY DESCRIPTION /tmp/DESCRIPTION
WORKDIR /tmp

RUN Rscript bootstrap.R

ADD . /code
WORKDIR /code

RUN Rscript ci.R