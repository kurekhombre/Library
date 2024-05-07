--CREATE DATABASE AND SCHEMA FOR THE PROJECT
-- Ensure the 'projects' database exists and switch to it
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'projects')
BEGIN
    EXEC('CREATE DATABASE projects');
END;
GO

USE projects;
GO

IF NOT EXISTS (SELECT * FROM information_schema.schemata WHERE schema_name = 'library_db')
BEGIN
    EXEC('CREATE SCHEMA library_db');
END;
GO

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'library_db' AND TABLE_NAME = 'Books')
BEGIN
	CREATE TABLE library_db.Books (
		ISBN VARCHAR(20) PRIMARY KEY,
		Title NVARCHAR(255) NOT NULL,
		Author NVARCHAR(255) NOT NULL,
		Publisher NVARCHAR(255),
		Category NVARCHAR(100)
	);
END;

GO

