-- Graf 1: 10 najpredávanejších skladieb
SELECT 
    t.`Name` AS `TrackName`, 
    SUM(f.`Quantity`) AS `TotalNumberOfSales`
FROM `Fact_Invoice` f
JOIN `Dim_Track` t ON f.`Dim_TrackId` = t.`Dim_TrackId`
GROUP BY t.`Name`
ORDER BY `TotalNumberOfSales` DESC
LIMIT 10;

-- Graf 2: Rozdelenie krajín podľa ziskovosti
SELECT 
    a.`BillingCountry` AS `CountryName`, 
    SUM(f.`TotalAmount`) AS `TotalAmount`
FROM `Fact_Invoice` f
JOIN `Dim_Address` a ON f.`Dim_AddressId` = a.`Dim_AddressId`
GROUP BY a.`BillingCountry`
ORDER BY `TotalAmount` DESC;

-- Graf 3: Dynamika predaja a príjmov podľa rokov
SELECT 
    d.`Year` AS `Year`, 
    SUM(f.`TotalAmount`) AS `TotalAmount`
FROM `Fact_Invoice` f
JOIN `Dim_Date` d ON f.`Dim_DateId` = d.`Dim_DateId`
GROUP BY d.`Year`
ORDER BY d.`Year` ASC;

-- Graf 4: Hodnotenie zamestnancov v závislosti od počtu spracovaných transakcií
SELECT 
    e.`FirstName` AS `FirstName`, 
    e.`LastName` AS `LastName`, 
    COUNT(f.`Fact_InvoiceId`) AS `TotalProcessedInvoices`
FROM `Fact_Invoice` f
JOIN `Dim_Employee` e ON f.`Dim_EmployeeId` = e.`Dim_EmployeeId`
GROUP BY e.`FirstName`, e.`LastName`
ORDER BY `TotalProcessedInvoices` DESC;

-- Graf 5: Najobľúbenejšie hudobné žánre podľa počtu predajov
SELECT 
    sub.`Genre` AS `GenreName`, 
    SUM(sub.`Quantity`) AS `TotalNumberOfSales`
FROM (
    SELECT DISTINCT 
        f.`Fact_InvoiceId`, 
        t.`Genre`, 
        f.`Quantity`
    FROM `Fact_Invoice` f
    JOIN `Dim_Track` t ON f.`Dim_TrackId` = t.`Dim_TrackId`
) sub
GROUP BY sub.`Genre`
ORDER BY `TotalNumberOfSales` DESC;
