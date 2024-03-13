
CREATE TABLE IF NOT EXISTS Authors (
  ID SERIAL PRIMARY KEY,
  Name VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS Users (
   ID SERIAL PRIMARY KEY,
   Name VARCHAR(100),
   Address VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS Books (
  ISBN VARCHAR(255) PRIMARY KEY,
  Title VARCHAR(255),
  Author INT,
  Publisher INT,
  Category INT,
  FOREIGN KEY (Author) REFERENCES Authors(ID)
);

CREATE TABLE IF NOT EXISTS Rents (
  ID SERIAL PRIMARY KEY,
  User_ID INT,
  Book VARCHAR(255),
  Rental_Date DATE,
  Return_Date DATE,
  FOREIGN KEY (User_ID) REFERENCES Users(ID),
  FOREIGN KEY (Book) REFERENCES Books(ISBN)
);

CREATE TABLE IF NOT EXISTS Reservations (
  ID SERIAL PRIMARY KEY,
  User_ID INT,
  Book VARCHAR(255),
  Rental_Date DATE,
  FOREIGN KEY (User_ID) REFERENCES Users(ID),
  FOREIGN KEY (Book) REFERENCES Books(ISBN)
);

CREATE TABLE IF NOT EXISTS Publishers (
  ID SERIAL PRIMARY KEY,
  Name VARCHAR(255),
  Address VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS Categories (
 ID SERIAL PRIMARY KEY,
 Category VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS Ratings (
 ID SERIAL PRIMARY KEY,
 User_ID INT,
 Book VARCHAR(255),
 Rating INT,
 FOREIGN KEY (User_ID) REFERENCES Users(ID),
 FOREIGN KEY (Book) REFERENCES Books(ISBN)
);

CREATE TABLE IF NOT EXISTS Rents_History (
 ID SERIAL PRIMARY KEY,
 User_ID INT,
 Book VARCHAR(255),
 Rental_Date DATE,
 Return_Date DATE,
 FOREIGN KEY (User_ID) REFERENCES Users(ID),
 FOREIGN KEY (Book) REFERENCES Books(ISBN)
);

INSERT INTO Authors (Name) VALUES ('Author 1');
INSERT INTO Authors (Name) VALUES ('Author 2');

INSERT INTO Users (Name, Address) VALUES ('User 1', 'Address 1');
INSERT INTO Users (Name, Address) VALUES ('User 2', 'Address 2');

INSERT INTO Books (ISBN, Title, Author, Publisher, Category) VALUES ('ISBN1', 'Book 1', 1, 1, 1);
INSERT INTO Books (ISBN, Title, Author, Publisher, Category) VALUES ('ISBN2', 'Book 2', 2, 1, 1);

INSERT INTO Rents (User_ID, Book, Rental_Date, Return_Date) VALUES (1, 'ISBN1', '2024-03-04', '2024-03-10');
INSERT INTO Rents (User_ID, Book, Rental_Date, Return_Date) VALUES (2, 'ISBN2', '2024-03-05', '2024-03-12');

INSERT INTO Reservations (User_ID, Book, Rental_Date) VALUES (1, 'ISBN2', '2024-03-06');
INSERT INTO Reservations (User_ID, Book, Rental_Date) VALUES (2, 'ISBN1', '2024-03-07');

INSERT INTO Publishers (Name, Address) VALUES ('Publisher 1', 'Publisher Address 1');
INSERT INTO Publishers (Name, Address) VALUES ('Publisher 2', 'Publisher Address 2');

INSERT INTO Categories (Category) VALUES ('Category 1');
INSERT INTO Categories (Category) VALUES ('Category 2');

INSERT INTO Ratings (User_ID, Book, Rating) VALUES (1, 'ISBN1', 4);
INSERT INTO Ratings (User_ID, Book, Rating) VALUES (2, 'ISBN2', 5);

INSERT INTO Rents_History (User_ID, Book, Rental_Date, Return_Date) VALUES (1, 'ISBN1', '2024-03-01', '2024-03-03');
INSERT INTO Rents_History (User_ID, Book, Rental_Date, Return_Date) VALUES (2, 'ISBN2', '2024-03-02', '2024-03-05');
