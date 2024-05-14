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

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'library_db' AND TABLE_NAME = 'Publishers')
BEGIN
	CREATE TABLE library_db.Publishers (
		[ID] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
		[Name] [varchar](100) NULL,
	);
END;

GO
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'library_db' AND TABLE_NAME = 'Categories')
BEGIN
	CREATE TABLE library_db.Categories (
		[ID] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
		[Name] [varchar](100) NULL,
	);
END;

GO

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'library_db' AND TABLE_NAME = 'Authors')
BEGIN
	CREATE TABLE library_db.Authors (
		[ID] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
		[Name] [varchar](100) NULL,
		[Address] [varchar](200) NULL
	);
END;

GO

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'library_db' AND TABLE_NAME = 'Books')
BEGIN
	CREATE TABLE library_db.Books (
		[ID] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
		ISBN VARCHAR(20),
		Title NVARCHAR(255) NOT NULL,
		Author int NOT NULL,
		Publisher int NOT NULL,
		Category int NOT NULL,
		CONSTRAINT FK_Books_Author FOREIGN KEY (Author) REFERENCES library_db.Authors (ID),
		CONSTRAINT FK_Books_Publisher FOREIGN KEY (Publisher) REFERENCES library_db.Publishers (ID),
		CONSTRAINT FK_Books_Category FOREIGN KEY (Category) REFERENCES library_db.Categories (ID)
	);
END;

GO

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'library_db' AND TABLE_NAME = 'Users')
BEGIN
	CREATE TABLE library_db.Users (
		[ID] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
		[Name] [varchar](100) NULL,
		[Address] [varchar](200) NULL
	);
END;

GO

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'library_db' AND TABLE_NAME = 'Ratings')
BEGIN
	CREATE TABLE library_db.Ratings (
		[ID] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
		UserID INT NOT NULL, 
		BookID INT NOT NULL,
		Rate INT CHECK (RATE BETWEEN 1 AND 5),
		Comment TEXT,
		CONSTRAINT FK_Ratings_User FOREIGN KEY (UserID)  REFERENCES library_db.Users(ID),
		CONSTRAINT FK_Ratings_Book FOREIGN KEY (BookID)  REFERENCES library_db.Books(ID)
	);
END;

GO

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'library_db' AND TABLE_NAME = 'Rents')
BEGIN
	CREATE TABLE library_db.Rents (
		ID INT IDENTITY(1,1) PRIMARY KEY,
		UserID INT NOT NULL,
		BookID INT NOT NULL,
		RentalDate DATETIME NOT NULL,
		ReturnDate DATETIME NULL,
		CONSTRAINT FK_Rents_User FOREIGN KEY (UserID) REFERENCES library_db.Users(ID),
		CONSTRAINT FK_Rents_Book FOREIGN KEY (BookID) REFERENCES library_db.Books(ID)
	);
END;

GO

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'library_db' AND TABLE_NAME = 'RentsHistory')
BEGIN
	CREATE TABLE library_db.RentsHistory (
		ID INT IDENTITY(1,1) PRIMARY KEY,
		UserID INT NOT NULL,
		BookID INT NOT NULL,
		RentalDate DATETIME NOT NULL,
		ReturnDate DATETIME NULL,
		CONSTRAINT FK_RentsHistory_User FOREIGN KEY (UserID) REFERENCES library_db.Users(ID),
		CONSTRAINT FK_RentsHistory_Book FOREIGN KEY (BookID) REFERENCES library_db.Books(ID)
	);
END;

GO

CREATE PROCEDURE MoveOldRentsToHistory
AS
BEGIN
    INSERT INTO library_db.RentsHistory (UserID, BookID, RentalDate, ReturnDate)
    SELECT UserID, BookID, RentalDate, ReturnDate
    FROM library_db.Rents
    WHERE ReturnDate IS NOT NULL AND ReturnDate <= DATEADD(YEAR, -1, GETDATE());

    DELETE FROM library_db.Rents
    WHERE ReturnDate IS NOT NULL AND ReturnDate IN (SELECT ReturnDate FROM library_db.RentsHistory WHERE ReturnDate <= DATEADD(YEAR, -1, GETDATE()));
END;
GO
