#!/bin/bash

/opt/mssql/bin/mssql-conf set network.tcpport 1433

/opt/mssql/bin/sqlservr &
pid="$!"

for i in {1..45}; do
    if /opt/mssql-tools/bin/sqlcmd -S localhost,1433 -U sa -P "YourStrongPassw0rd" -Q "SELECT 1" &>/dev/null; then
        echo "SQL Server is up! Running the setup script."
        
        /opt/mssql-tools/bin/sqlcmd -S localhost,1433 -U sa -P "YourStrongPassw0rd" master -i /usr/src/app/create_queries.sql
        /opt/mssql-tools/bin/sqlcmd -S localhost,1433 -U sa -P "YourStrongPassw0rd" master -i /usr/src/app/insert_queries.sql

        break
    else
        echo "SQL Server is starting up. Attempt $i of 45."
        sleep 2
    fi
done

if ! /opt/mssql-tools/bin/sqlcmd -S localhost,1433 -U sa -P "YourStrongPassw0rd" -Q "SELECT 1" &>/dev/null; then
    echo "Failed to connect to SQL Server withing 90seconds."
fi

wait $pid