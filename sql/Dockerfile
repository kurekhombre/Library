# Use the official SQL Server 2019 image from Microsoft
FROM mcr.microsoft.com/mssql/server:2019-latest

# Set environment variables for SQL Server
ENV ACCEPT_EULA=Y \
    SA_PASSWORD=YourStrongPassw0rd

# Expose the default SQL Server port
EXPOSE 1433

# Copy entrypoint script into the container
COPY sql/entrypoint.sh /usr/src/app/

COPY sql/queries/create_queries.sql /usr/src/app/
COPY sql/queries/insert_queries.sql /usr/src/app/

# Give permissions to execute the entrypoint script
USER root
RUN chmod +x /usr/src/app/entrypoint.sh
USER mssql

# Set entrypoint script to run on container start
ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
