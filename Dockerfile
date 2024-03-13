FROM mcr.microsoft.com/mssql/server

ENV ACCEPT_EULA=Y \
    SA_PASSWORD=YourStrongPassw0rd

EXPOSE 1433

COPY scripts/entrypoint.sh /usr/src/app/
COPY dataset/create_queries.sql /usr/src/app
COPY dataset/insert_queries.sql /usr/src/app

USER root
RUN chmod +x /usr/src/app/entrypoint.sh
USER mssql

ENTRYPOINT ["/usr/src/app/entrypoint.sh"]