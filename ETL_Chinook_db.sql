-- Vytvorenie databázy
CREATE DATABASE BISON_CHINOOK;
CREATE SCHEMA BISON_CHINOOK_SCHEMA;
USE DATABASE BISON_CHINOOK;
USE SCHEMA BISON_CHINOOK_SCHEMA;

-- Vytvorenie tabuľky
CREATE TABLE `Album`
(
    `AlbumId` INT NOT NULL,
    `Title` NVARCHAR(160) NOT NULL,
    `ArtistId` INT NOT NULL,
    CONSTRAINT `PK_Album` PRIMARY KEY  (`AlbumId`)
);

CREATE TABLE `Artist`
(
    `ArtistId` INT NOT NULL,
    `Name` NVARCHAR(120),
    CONSTRAINT `PK_Artist` PRIMARY KEY  (`ArtistId`)
);

CREATE TABLE `Customer`
(
    `CustomerId` INT NOT NULL,
    `FirstName` NVARCHAR(40) NOT NULL,
    `LastName` NVARCHAR(20) NOT NULL,
    `Company` NVARCHAR(80),
    `Address` NVARCHAR(70),
    `City` NVARCHAR(40),
    `State` NVARCHAR(40),
    `Country` NVARCHAR(40),
    `PostalCode` NVARCHAR(10),
    `Phone` NVARCHAR(24),
    `Fax` NVARCHAR(24),
    `Email` NVARCHAR(60) NOT NULL,
    `SupportRepId` INT,
    CONSTRAINT `PK_Customer` PRIMARY KEY  (`CustomerId`)
);

CREATE TABLE `Employee`
(
    `EmployeeId` INT NOT NULL,
    `LastName` NVARCHAR(20) NOT NULL,
    `FirstName` NVARCHAR(20) NOT NULL,
    `Title` NVARCHAR(30),
    `ReportsTo` INT,
    `BirthDate` DATETIME,
    `HireDate` DATETIME,
    `Address` NVARCHAR(70),
    `City` NVARCHAR(40),
    `State` NVARCHAR(40),
    `Country` NVARCHAR(40),
    `PostalCode` NVARCHAR(10),
    `Phone` NVARCHAR(24),
    `Fax` NVARCHAR(24),
    `Email` NVARCHAR(60),
    CONSTRAINT `PK_Employee` PRIMARY KEY  (`EmployeeId`)
);

CREATE TABLE `Genre`
(
    `GenreId` INT NOT NULL,
    `Name` NVARCHAR(120),
    CONSTRAINT `PK_Genre` PRIMARY KEY  (`GenreId`)
);

CREATE TABLE `Invoice`
(
    `InvoiceId` INT NOT NULL,
    `CustomerId` INT NOT NULL,
    `InvoiceDate` DATETIME NOT NULL,
    `BillingAddress` NVARCHAR(70),
    `BillingCity` NVARCHAR(40),
    `BillingState` NVARCHAR(40),
    `BillingCountry` NVARCHAR(40),
    `BillingPostalCode` NVARCHAR(10),
    `Total` NUMERIC(10,2) NOT NULL,
    CONSTRAINT `PK_Invoice` PRIMARY KEY  (`InvoiceId`)
);

CREATE TABLE `InvoiceLine`
(
    `InvoiceLineId` INT NOT NULL,
    `InvoiceId` INT NOT NULL,
    `TrackId` INT NOT NULL,
    `UnitPrice` NUMERIC(10,2) NOT NULL,
    `Quantity` INT NOT NULL,
    CONSTRAINT `PK_InvoiceLine` PRIMARY KEY  (`InvoiceLineId`)
);

CREATE TABLE `MediaType`
(
    `MediaTypeId` INT NOT NULL,
    `Name` NVARCHAR(120),
    CONSTRAINT `PK_MediaType` PRIMARY KEY  (`MediaTypeId`)
);

CREATE TABLE `Playlist`
(
    `PlaylistId` INT NOT NULL,
    `Name` NVARCHAR(120),
    CONSTRAINT `PK_Playlist` PRIMARY KEY  (`PlaylistId`)
);

CREATE TABLE `PlaylistTrack`
(
    `PlaylistId` INT NOT NULL,
    `TrackId` INT NOT NULL,
    CONSTRAINT `PK_PlaylistTrack` PRIMARY KEY  (`PlaylistId`, `TrackId`)
);

CREATE TABLE `Track`
(
    `TrackId` INT NOT NULL,
    `Name` NVARCHAR(200) NOT NULL,
    `AlbumId` INT,
    `MediaTypeId` INT NOT NULL,
    `GenreId` INT,
    `Composer` NVARCHAR(220),
    `Milliseconds` INT NOT NULL,
    `Bytes` INT,
    `UnitPrice` NUMERIC(10,2) NOT NULL,
    CONSTRAINT `PK_Track` PRIMARY KEY  (`TrackId`)
);

ALTER TABLE `Album` ADD CONSTRAINT `FK_AlbumArtistId`
    FOREIGN KEY (`ArtistId`) REFERENCES `Artist` (`ArtistId`) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE `Customer` ADD CONSTRAINT `FK_CustomerSupportRepId`
    FOREIGN KEY (`SupportRepId`) REFERENCES `Employee` (`EmployeeId`) ON DELETE NO ACTION ON UPDATE NO ACTION;


ALTER TABLE `Employee` ADD CONSTRAINT `FK_EmployeeReportsTo`
    FOREIGN KEY (`ReportsTo`) REFERENCES `Employee` (`EmployeeId`) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE `Invoice` ADD CONSTRAINT `FK_InvoiceCustomerId`
    FOREIGN KEY (`CustomerId`) REFERENCES `Customer` (`CustomerId`) ON DELETE NO ACTION ON UPDATE NO ACTION;


ALTER TABLE `InvoiceLine` ADD CONSTRAINT `FK_InvoiceLineInvoiceId`
    FOREIGN KEY (`InvoiceId`) REFERENCES `Invoice` (`InvoiceId`) ON DELETE NO ACTION ON UPDATE NO ACTION;


ALTER TABLE `InvoiceLine` ADD CONSTRAINT `FK_InvoiceLineTrackId`
    FOREIGN KEY (`TrackId`) REFERENCES `Track` (`TrackId`) ON DELETE NO ACTION ON UPDATE NO ACTION;


ALTER TABLE `PlaylistTrack` ADD CONSTRAINT `FK_PlaylistTrackPlaylistId`
    FOREIGN KEY (`PlaylistId`) REFERENCES `Playlist` (`PlaylistId`) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE `PlaylistTrack` ADD CONSTRAINT `FK_PlaylistTrackTrackId`
    FOREIGN KEY (`TrackId`) REFERENCES `Track` (`TrackId`) ON DELETE NO ACTION ON UPDATE NO ACTION;


ALTER TABLE `Track` ADD CONSTRAINT `FK_TrackAlbumId`
    FOREIGN KEY (`AlbumId`) REFERENCES `Album` (`AlbumId`) ON DELETE NO ACTION ON UPDATE NO ACTION;


ALTER TABLE `Track` ADD CONSTRAINT `FK_TrackGenreId`
    FOREIGN KEY (`GenreId`) REFERENCES `Genre` (`GenreId`) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE `Track` ADD CONSTRAINT `FK_TrackMediaTypeId`
    FOREIGN KEY (`MediaTypeId`) REFERENCES `MediaType` (`MediaTypeId`) ON DELETE NO ACTION ON UPDATE NO ACTION;


-- Vytvorenie BISON_Chinook_stage pre .csv súbory
CREATE OR REPLACE STAGE BISON_Chinook_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');

COPY INTO `Album`
FROM @BISON_Chinook_stage/Album.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO `Artist`
FROM @BISON_Chinook_stage/Artist.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO `Customer`
FROM @BISON_Chinook_stage/Customer.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO `Employee`
FROM @BISON_Chinook_stage/Employee.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO `Genre`
FROM @BISON_Chinook_stage/Genre.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO `Invoice`
FROM @BISON_Chinook_stage/Invoice.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO `InvoiceLine`
FROM @BISON_Chinook_stage/InvoiceLine.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO `MediaType`
FROM @BISON_Chinook_stage/MediaType.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO `Playlist`
FROM @BISON_Chinook_stage/Playlist.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO `PlaylistTrack`
FROM @BISON_Chinook_stage/PlaylistTrack.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO `Track`
FROM @BISON_Chinook_stage/Track.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

--- ELT - (T)ransform
CREATE TABLE `Dim_Employee` (
    `Dim_EmployeeId` INT,
    `LastName` VARCHAR(20),
    `FirstName` VARCHAR(20),
    `Title` VARCHAR(30),
    `HireDate` TIMESTAMP_NTZ(9),
    `City` VARCHAR(40),
    `State` VARCHAR(40),
    `Country` VARCHAR(40),
    `Email` VARCHAR(60),
    `StartDate` DATE,
    `EndDate` DATE,
    `IsCurrent` BOOLEAN
);

INSERT INTO `Dim_Employee`
SELECT 
    e.`EmployeeId` AS `Dim_EmployeeId`,
    e.`LastName`,
    e.`FirstName`,
    e.`Title`,
    e.`HireDate`,
    e.`City`,
    e.`State`,
    e.`Country`,
    e.`Email`,
    CURRENT_DATE AS `StartDate`,
    NULL AS `EndDate`,
    TRUE AS `IsCurrent`
FROM `Employee` e;

CREATE TABLE `Dim_Customer` (
    `Dim_CustomerId` INT,
    `FirstName` VARCHAR(20),
    `LastName` VARCHAR(20),
    `City` VARCHAR(40),
    `State` VARCHAR(40),
    `Country` VARCHAR(40),
    `Email` VARCHAR(60),
    `StartDate` DATE,
    `EndDate` DATE,
    `IsCurrent` BOOLEAN
);

INSERT INTO `Dim_Customer`
SELECT 
    c.`CustomerId` AS `Dim_CustomerId`,
    c.`FirstName`,
    c.`LastName`,
    c.`City`,
    c.`State`,
    c.`Country`,
    c.`Email`,
    CURRENT_DATE AS `StartDate`,
    NULL AS `EndDate`,
    TRUE AS `IsCurrent`
FROM `Customer` c;


CREATE TABLE `Dim_Track` AS
SELECT DISTINCT
    t.`TrackId` AS `Dim_TrackId`,
    t.`Name`,
    t.`Composer`,
    t.`Milliseconds`,
    t.`Bytes`,
    mt.`Name` AS `MediaType`,
    al.`Title` AS `Album`,
    ar.`Name` AS `Artist`,
    g.`Name` AS `Genre`,
    p.`Name` AS `Playlist`
FROM `Track` t
LEFT JOIN `MediaType` mt ON t.`MediaTypeId` = mt.`MediaTypeId`
LEFT JOIN `Album` al ON t.`AlbumId` = al.`AlbumId`
LEFT JOIN `Artist` ar ON al.`ArtistId` = ar.`ArtistId`
LEFT JOIN `Genre` g ON t.`GenreId` = g.`GenreId`
LEFT JOIN `PlaylistTrack` pt ON t.`TrackId` = pt.`TrackId`
LEFT JOIN `Playlist` p ON pt.`PlaylistId` = p.`PlaylistId`;


CREATE TABLE `Dim_Address` AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY `BillingCity`, `BillingState`, `BillingCountry`) AS `Dim_AddressId`,
    `BillingCity`,
    `BillingState`,
    `BillingCountry`
FROM (
    SELECT DISTINCT
        `BillingCity`,
        `BillingState`,
        `BillingCountry`
    FROM `Invoice`
) AS unique_addresses;

CREATE TABLE `Dim_Date` AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY CAST(i.`InvoiceDate` AS DATE)) AS `Dim_DateId`,
    DATE(i.`InvoiceDate`) AS `Date`,                 
    EXTRACT(YEAR FROM i.`InvoiceDate`) AS `Year`,    
    EXTRACT(MONTH FROM i.`InvoiceDate`) AS `Month`, 
    CASE 
        WHEN EXTRACT(MONTH FROM i.`InvoiceDate`) = 1 THEN 'Január'
        WHEN EXTRACT(MONTH FROM i.`InvoiceDate`) = 2 THEN 'Február'
        WHEN EXTRACT(MONTH FROM i.`InvoiceDate`) = 3 THEN 'Marec'
        WHEN EXTRACT(MONTH FROM i.`InvoiceDate`) = 4 THEN 'Apríl'
        WHEN EXTRACT(MONTH FROM i.`InvoiceDate`) = 5 THEN 'Máj'
        WHEN EXTRACT(MONTH FROM i.`InvoiceDate`) = 6 THEN 'Jún'
        WHEN EXTRACT(MONTH FROM i.`InvoiceDate`) = 7 THEN 'Júl'
        WHEN EXTRACT(MONTH FROM i.`InvoiceDate`) = 8 THEN 'August'
        WHEN EXTRACT(MONTH FROM i.`InvoiceDate`) = 9 THEN 'September'
        WHEN EXTRACT(MONTH FROM i.`InvoiceDate`) = 10 THEN 'Október'
        WHEN EXTRACT(MONTH FROM i.`InvoiceDate`) = 11 THEN 'November'
        WHEN EXTRACT(MONTH FROM i.`InvoiceDate`) = 12 THEN 'December'
        ELSE 'Unknown'
    END AS `MonthName`,
    EXTRACT(QUARTER FROM i.`InvoiceDate`) AS `Quarter`,
    EXTRACT(DAY FROM i.`InvoiceDate`) AS `Day`,    
    CASE 
        WHEN EXTRACT(DOW FROM i.`InvoiceDate`) = 1 THEN 'Pondelok'
        WHEN EXTRACT(DOW FROM i.`InvoiceDate`) = 2 THEN 'Utorok'
        WHEN EXTRACT(DOW FROM i.`InvoiceDate`) = 3 THEN 'Streda'
        WHEN EXTRACT(DOW FROM i.`InvoiceDate`) = 4 THEN 'Štvrtok'
        WHEN EXTRACT(DOW FROM i.`InvoiceDate`) = 5 THEN 'Piatok'
        WHEN EXTRACT(DOW FROM i.`InvoiceDate`) = 6 THEN 'Sobota'
        WHEN EXTRACT(DOW FROM i.`InvoiceDate`) = 7 THEN 'Nedeľa'
        ELSE 'Unknown'
    END AS `WeekDay`,
    CASE 
        WHEN EXTRACT(DOW FROM i.`InvoiceDate`) IN (6, 7) THEN 'Víkend'
        ELSE 'Pracovný deň'
    END AS `IsWeekend`                          
FROM (SELECT DISTINCT DATE(`InvoiceDate`) AS `InvoiceDate` FROM `Invoice`) i;

CREATE TABLE `Fact_Invoice` AS
SELECT 
    il.`InvoiceLineId` AS `Fact_InvoiceId`,
    il.`Quantity`,
    il.`UnitPrice`,
    il.`Quantity` * il.`UnitPrice` AS `TotalAmount`,
    t.`TrackId` AS `Dim_TrackId`,
    c.`CustomerId` AS `Dim_CustomerId`,
    e.`EmployeeId` AS `Dim_EmployeeId`,
    da.`Dim_AddressId` AS `Dim_AddressId`,
    d.`Dim_DateId` AS `Dim_DateId`         
FROM `InvoiceLine` il
JOIN `Track` t ON il.`TrackId` = t.`TrackId`
JOIN `Invoice` i ON il.`InvoiceId` = i.`InvoiceId`
JOIN `Customer` c ON i.`CustomerId` = c.`CustomerId`
JOIN `Employee` e ON c.`SupportRepId` = e.`EmployeeId`
JOIN `Dim_Address` da ON i.`BillingCity` = da.`BillingCity`
JOIN `Dim_Date` d ON DATE(i.`InvoiceDate`) = d.`Date`
ORDER BY `Fact_InvoiceId`;

-- DROP stagging tables
DROP TABLE IF EXISTS `InvoiceLine`;
DROP TABLE IF EXISTS `Invoice`;
DROP TABLE IF EXISTS `Artist`;
DROP TABLE IF EXISTS `Album`;
DROP TABLE IF EXISTS `Playlist`;
DROP TABLE IF EXISTS `Genre`;
DROP TABLE IF EXISTS `MediaType`;
DROP TABLE IF EXISTS `Customer`;
DROP TABLE IF EXISTS `Employee`;
DROP TABLE IF EXISTS `Track`;
DROP TABLE IF EXISTS `PlaylistTrack`;


