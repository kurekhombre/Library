USE projects;
GO
INSERT INTO library_db.Users (Name, Address)
VALUES 
('John Doe', 'Switzerland'),
('Johan Doge', 'USA');

INSERT INTO library_db.Authors (Name, Address)
VALUES 
('Dan X', 'Canada'),
('Karl Braun', 'Poland');

INSERT INTO library_db.Categories (Name)
VALUES 
('Fiction'),
('Sci-fi'),
('Drama');

INSERT INTO library_db.Publishers (Name)
VALUES 
('Oreily'),
('XYZ');

INSERT INTO library_db.Books (ISBN, Title, Author, Publisher, Category)
VALUES 
('9783161484100', 'The Great Gatsby', '1', '1', '1'),
('9780141182803', '1984', '1','1', '2'),
('9780394823792', 'To Kill a Mockingbird', '2', '2', '2'),
('9780207184626', 'The Book Thief', '1', '1', '3'),
('9780061120060', 'Brave New World', '2', '2', '3');


INSERT INTO library_db.Ratings(UserID, BookID,Rate,Comment) VALUES 
('1','3', '5', 'Great book!'),
('2','4', '3','OK');

INSERT INTO library_db.Rents (UserID, BookID, RentalDate, ReturnDate)
VALUES
    ('1', '2', '2024-05-01 10:00:00', '2024-05-10 10:00:00'),
    ('2', '3', '2024-05-02 12:00:00', '2024-05-12 12:00:00'),
    ('2', '1', '2024-05-03 14:00:00', NULL);  
