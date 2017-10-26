FROM 996097627176.dkr.ecr.us-east-1.amazonaws.com/data-zapier-nightly:latest

ENV ENVIRONMENT=production
ENV TARBALL=${PKG}_${VERSION}.tar.gz
ENV REDSHIFT_ROLE='arn:aws:iam::996097627176:role/production-redshift'

ADD . /code
WORKDIR /code

RUN R CMD build .
RUN R CMD INSTALL ${TARBALL}

RUN mv ${TARBALL} / && rm -rf * && mv /${TARBALL} .

CMD R CMD check ${TARBALL}