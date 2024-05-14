#!/bin/bash

# Configure SQL Server
/opt/mssql/bin/mssql-conf set network.tcpport 1433

# Start SQL Server
/opt/mssql/bin/sqlservr & 
pid="$!"

# Wait for SQL Server to start
# Replace <YourStrong!Passw0rd> with your actual SA password
# This loop will check if SQL Server is up every 2 seconds, for a 90 seconds timeout
for i in {1..45}; do
    if /opt/mssql-tools/bin/sqlcmd -S localhost,1433 -U sa -P "YourStrongPassw0rd" -Q "SELECT 1" &>/dev/null; then
        echo "SQL Server is up! Running the setup script."

        # Check if the initialization has already been completed
        if [ ! -f "/usr/src/app/.init_db_done" ]; then
            echo "Database initialization started."

            # Run the SQL setup script
            /opt/mssql-tools/bin/sqlcmd -S localhost,1433 -U sa -P "YourStrongPassw0rd" -i /usr/src/app/create_queries.sql
            /opt/mssql-tools/bin/sqlcmd -S localhost,1433 -U sa -P "YourStrongPassw0rd" -i /usr/src/app/insert_queries.sql
            
            # Mark this init as done
            touch /usr/src/app/.init_db_done
            echo "Database initialization completed."
        else
            echo "Database initialization already completed."
        fi
        
        # Break the loop since we were able to connect and run the query
        break
    else
        echo "SQL Server is starting up. Attempt $i of 45."
        sleep 2
    fi
done

# If we couldn't connect after 90 seconds, fail the script
if ! /opt/mssql-tools/bin/sqlcmd -S localhost,1433 -U sa -P "YourStrongPassw0rd" -Q "SELECT 1" &>/dev/null; then
    echo "Failed to connect to SQL Server within 90 seconds."
fi

# Wait for the SQL Server process to finish
wait $pid
