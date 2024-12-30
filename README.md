# **ETL proces datasetu Chinook**

Toto úložisko obsahuje implementáciu procesu ETL spoločnosti Snowflake na analýzu údajov zo súboru údajov **Chinook**. Cieľom projektu je analyzovať údaje hudobného obchodu Chinook s cieľom pochopiť vzorce predaja, správanie používateľov, popularitu hudobných žánrov a výkonnosť zamestnancov. Proces ETL pomôže pripraviť údaje na viacrozmernú analýzu a vizualizáciu kľúčových ukazovateľov.

____

## **1. Úvod a popis zdrojových dát**

Cieľom semestrálneho projektu je analyzovať údaje týkajúce sa hudobných skladieb, používateľov a ich nákupov. Táto analýza nám umožňuje identifikovať trendy v hudobných preferenciách zákazníkov, najobľúbenejšie skladby, žánre a zoznamy skladieb, ako aj vyhodnotiť produktivitu zamestnancov a efektivitu predaja.

Surové údaje sú relačná databáza obsahujúca informácie o:

+ Predaj hudobných skladieb.
+ Zákazníkoch a ich objednávkach.
+ Umelci, albumy a žánre.
+ Zamestnanci a zoznamy skladieb.

Účel analýzy:

+ Identifikovať najobľúbenejšie skladby, žánre a zoznamy skladieb.
+ Identifikovať geografické trendy v nákupoch.
+ Analyzovať údaje o predaji a ziskoch.
+ Vyhodnotiť produktivitu zamestnancov.

____

### **1.1 Základný opis tabuľky**

+ **Artist**: Obsahuje informácie o umelcovi. Obsahuje jedinečný identifikátor a názov.
+ **Album**: Ukladá údaje o hudobných albumoch spojených s umelcami.
+ **Track**: Hlavná tabuľka s informáciami o skladbe vrátane žánru, albumu, ceny a trvania.
+ **Genre**: Zoznam hudobných žánrov.
+ **MediaType**: Opis dostupných formátov mediálneho obsahu.
+ **Playlist**: Zoznam zoznamov skladieb vytvorených používateľmi.
+ **PlaylistTrack**: Prepojenie skladieb so zoznamami skladieb.
+ **Invoice**: Informácie o objednávkach zákazníkov.
+ **InvoiceLine**</mark>: Podrobnosti o každom riadku objednávky.
+ **Customer**: Údaje o zákazníkovi vrátane kontaktných informácií.
+ **Employee**: Informácie o zamestnancoch.

____

### **1.2 Dátová architektúra**

### **ERD diagram**

Surové dáta sa usporiadajú do relačného modelu, ktorý je reprezentovaný ako **entitno-relačný diagram (ERD)**:

<p align="center">
  <a href="Chinook_ERD.png">
    <img src="Chinook_ERD.png" alt="Obrázok 1 Entitno-relačná schéma Chinook">
  </a>
  <br>
 Obrázok 1 Entitno-relačná schéma Chinook
</p>

____

## **2. Dimenzionálny model**

Na efektívnu analýzu bol navrhnutý **hviezdicový model (hviezdicová schéma)**, ktorého stredobodom je tabuľka **Fact_Invoice** obsahujúca informácie o predaji hudobných skladieb.

Hlavné metriky v tabuľke fact sú:

+ **Fact_InvoiceId**: jedinečný kľúč faktúry.
+ **Quantity**: Počet zakúpených skladieb.
+ **UnitPrice**: Cena za skladbu.
+ **TotalAmount**: Celková suma faktúry.
+ **Dim_TrackId**: Odkaz na hudobnú skladbu spojenú s týmto plemenom z dimenzionálnej tabuľky **Dim_Track**.
+ **Dim_DateId**: Odkaz na dátum spojený s transakciou z dimenzionálnej tabuľky **Dim_Date**.
+ **Dim_CustomerId**: Odkaz na zákazníka z dimenzionálnej tabuľky **Dim_Customer**. 
+ **Dim_AddressId**: Odkaz na adresu, na ktorej bol nákup uskutočnený, z dimenzionálnej tabuľky **Dim_Address**.
+ **Dim_EmployeeId**: Odkaz na zamestnanca z tabuľky **Dim_Employee**.

Faktová tabuľka **Fact_Invoice** je prepojená s nasledujúcimi dimenziami:

+ **Dim_Customer**: Obsahuje informácie o zákazníkovi (jedinečné ID zákazníka, meno, adresu, kontaktné údaje atď.). Tabuľka **Fact_Invoice** používa pole **Dim_CustomerId** na komunikáciu s tabuľkou **Dim_Customer**. Toto pole udáva, ktorý zákazník uskutočnil nákup. **Dimenzia typu 2 (SCD2)** - Predpokladá sa, že zákazníci môžu meniť informácie (napríklad adresu alebo kontaktné údaje) a ukladá sa história zmien.
+ **Dim_Employee**: Obsahuje informácie o zamestnancoch (meno, priezvisko, titul, adresa a ďalšie údaje). Tabuľka **Fact_Invoice** používa pole **Dim_EmployeeId** na prepojenie s tabuľkou **Dim_Employee**, aby uviedla, ktorý zamestnanec spracoval nákup. **Dimenzia typu 2 (SCD2)** - Informácie o zamestnancovi sa môžu meniť (napríklad zmena názvu pracovnej pozície alebo adresy) a tieto zmeny sa musia zachovať.
+ **Dim_Track**: Obsahuje údaje o hudobnej skladbe (názov, trvanie, žáner a ďalšie atribúty). Tabuľka **Fact_Invoice** používa pole **Dim_TrackId** na prepojenie s tabuľkou **Dim_Track**, aby uviedla, ktorá hudobná skladba bola predaná. **Dimenzia typu 0 (SCD0)** - zmeny informácií o skladbe si nevyžadujú uloženie historických údajov, pretože neovplyvňujú analýzu.
+ **Dim_Address**: Obsahuje informácie o adrese transakcie. Tabuľka **Fact_Invoice** používa pole **Dim_AddressId** na prepojenie s tabuľkou **Dim_Address**, ktorá sa používa na určenie adresy, na ktorej sa uskutočnil nákup na trati. **Dimenzia typu 0 (SCD0)** - adresa uvádza len miesto, kde bol nákup uskutočnený, a nemení sa, história zmien nie je potrebná, pre každú transakciu je aktuálna adresa pevne stanovená.
+ **Dim_Date**: Obsahuje dátum nákupu (deň, mesiac, rok, štvrťrok, deň v týždni atď.). Tabuľka **Fact_Invoice** používa pole **Dim_DateId** na označenie dátumu uskutočnenia nákupu. **Dimenzia typu 0 (SCD0)** - údaje kalendára sa nemenia, takže nie je potrebné uchovávať históriu zmien.

Štruktúra modelu hviezdy je znázornená na nasledujúcom obrázku. Diagram znázorňuje vzťahy medzi tabuľkou faktov a meraniami, čo uľahčuje pochopenie a implementáciu modelu.

<p align="center">
  <a href="Chinook_Star_schema.png">
    <img src="Chinook_Star_schema.png" alt="Obrázok 2 Schéma hviezdy pre Chinook">
  </a>
  <br>
  Obrázok 2 Schéma hviezdy pre Chinook
</p>

____

## **3. ETL proces v Snowflake**

ETL proces pozostával z troch hlavných fáz: **extrahovanie** (Extract), **transformácia** (Transform) a **načítanie** (Load). Tento proces bol implementovaný v systéme Snowflake na prípravu surových údajov z etapovej vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

____

### **3.1 Extract (Extrahovanie dát)**

V prvom kroku procesu ETL sa údaje extrahujú z externého zdroja a načítajú do programu **Snowflake** pomocou príkazov SQL, ktoré sa vykonávajú priamo v **Worksheets**. To sa vykoná importovaním súboru **Chinook_MySql.sql** do **Worksheets** a následným vykonaním všetkých príkazov.

Základné príkazy SQL používané v procese **Extract**:

1.	**CREATE DATABASE/SCHEMA** a **USE DATABASE/SCHEMA**

Príkazy **CREATE DATABASE/SCHEMA** sa používajú na vytvorenie databázy **CHINOOK** a jej schémy v programe **Snowflake**. A príkazy **USE DATABASE/SCHEMA** umožňujú používať vytvorenú databázu a schému na ďalšie transformácie.

Príklady príkazov:
```sql 
CREATE DATABASE BISON_CHINOOK;
CREATE SCHEMA BISON_CHINOOK_SCHEMA;
USE DATABASE BISON_CHINOOK;
USE SCHEMA BISON_CHINOOK_SCHEMA;
```

2.	**CREATE TABLE**

Príkaz **CREATE TABLE** sa používa na vytvorenie štruktúry tabuľky v databáze. Vytvorí sa napríklad tabuľka **Album**, ktorá bude obsahovať informácie o hudobných albumoch. Príkaz definuje štruktúru tabuľky vrátane názvov stĺpcov, ich dátových typov, obmedzení a kľúčov.

Príklad príkazu:
```sql 
CREATE TABLE `Album`.
(
    `AlbumId` INT NOT NULL,
    `Title` VARCHAR(160) NOT NULL,
    `ArtistId` INT NOT NULL,
    CONSTRAINT `PK_Album` PRIMARY KEY (`AlbumId`)
);
```
Ostatné tabuľky sa vytvoria rovnakým spôsobom.

3. **INSERT INTO**

Príkaz **INSERT INTO** sa používa na pridanie údajov do tabuľky. Tento príkaz sa používa napríklad na vloženie údajov do tabuľky **Genre**, ktorá obsahuje informácie o hudobných žánroch.

Príklad príkazu:
```sql 
INSERT INTO `Genre` (`GenreId`, `Name`) VALUES
    (1, 'Rock'),
    (2, „Jazz“),
    (3, „Metal“),
    (4, „Alternative & Punk“),
    (5, 'Rock And Roll'),
    (6, „Blues“),
    (7, „Lati“),
    (8, „Reggae“),
    (9, „Pop“),
    (10, „Soundtrack“),
    (11, „Bossa Nova“),
    (12, „Easy Listening“),
    (13, „Heavy Metal“),
    (14, „R&B/Soul“),
    (15, „Electronica/Dance“),
    (16, „World“),
    (17, „Hip Hop/Rap“),
    (18, „Science Fictio“),
    (19, „Televízne programy“),
    (20, „Sci Fi & Fantasy“),
    (21, „Dráma“),
    (22, „Komédie“),
    (23, „Alternatívne“),
    (24, „Klasika“),
    (25, „Opera“);
```
Zvyšné tabuľky sa vyplnia rovnakým spôsobom.

Týmto spôsobom boli vytvorené štruktúry tabuliek databázy **CHINOOK** a naplnené údajmi.

____

### **3.2 Transfor (Transformácia dát)**

V tomto kroku sa údaje získané z exporovaných tabuliek vyčistili, transformovali a obohatili. Hlavným cieľom bolo pripraviť dimenzie a tabuľku faktov, ktoré by umožnili jednoduché a efektívne analýzy.

Dimenzie boli navrhnuté na poskytovanie kontextu pre faktovú tabuľku. Dimenzia **Dim_Employee** obsahuje informácie o zamestnancoch predajne vrátane údajov ako meno, priezvisko, pozícia, dátum povýšenia, e-mail a ich adresy.
Typ **dimenzie 2 (SCD2)** - informácie o zamestnancovi sa môžu meniť (napr. zmena pracovnej pozície alebo adresy) a tieto zmeny sa musia uložiť. Preto sa pre tento typ dimenzie musia pridať ďalšie stĺpce:
+ **StartDate** - dátum začiatku záznamu.
+ **EndDate** - dátum ukončenia platnosti záznamu (zvyčajne NULL, ak je záznam aktuálny).
+ **IsCurrent** - príznak označujúci relevantnosť záznamu (napríklad 1 pre aktuálny záznam a 0 pre neaktuálny záznam).
V budúcnosti pomôže pri načítavaní údajov skontrolovať, či sa údaje zmenili, a ak áno - vytvoriť nový záznam s aktualizovanými hodnotami a starý záznam uložiť do histórie.

Príklad kódu:
```sql 
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
```
CURRENT_DATE sa používa na zaznamenanie dátumu aktuálnej zmeny v tabuľke.

Rovnakým spôsobom sa vytvorí dimenzia **Dim_Customer**, ktorá obsahuje informácie o zákazníkoch, ako je jedinečný identifikátor zákazníka, meno, adresa, kontaktné údaje atď. Typ dimenzie je rovnaký - **typ 2 (SCD2)**, pretože sa predpokladá, že zákazníci môžu meniť informácie (napríklad adresu alebo kontaktné údaje) a mala by sa ukladať história zmien.

Dimenzia **Dim_Date** je určená na ukladanie informácií o dátume nákupu skladby. Obsahuje odvodené údaje, ako je deň, mesiac, rok, deň v týždni (v textovom aj číselnom formáte) a štvrťrok. Štruktúra tejto dimenzie umožňuje podrobné časové analýzy, napríklad najvyšší počet predajov podľa dňa, mesiaca alebo roka. Z hľadiska SCD je táto dimenzia kategorizovaná ako **SCD typ 0**. To znamená, že existujúce záznamy v tejto dimenzii sú nemenné a obsahujú statické informácie.

Príklad kódu:
```sql 
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
FROM `Invoice` i;
```
Transformácia zahŕňala pridanie názvu mesiaca a dňa v týždni spolu s popisom typu dňa (víkend alebo nie). Boli pridané aj ďalšie typy dátumov. Dimenzie **Dim_Address** a **Dim_Track** majú rovnakú dimenziu **SCD typ 0** a boli vytvorené rovnakým spôsobom.

Tabuľka **Fact_Invoice** obsahuje informácie o počte predaných skladieb, cene za 1 skladbu a celkovej sume za všetky predané kópie. Obsahuje aj odkazy na všetky dimenzie.
```sql 
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
JOIN `Dim_Date` d ON DATE(i.`InvoiceDate`) = d.`Date`;
```

____

### **3.3 Load (Načítanie dát)**

Po úspešnom vytvorení dimenzie a tabuľky faktov sa údaje načítali do konečnej štruktúry. Nakoniec sa exportované tabuľky vymazali, aby sa optimalizovalo využitie úložiska:
```sql 
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
```

Proces ETL v softvéri **Snowflake** umožnil prepracovať nespracované údaje zo súboru **Chinook_MySql** do viacrozmerného modelu typu hviezda. Tento proces zahŕňal čistenie, obohacovanie a reorganizáciu údajov. Výsledný model umožňuje analyzovať preferencie poslucháčov a kúpnu silu a poskytuje základ pre vizualizácie a reporty.

____

## **4. Vizualizácia dát**










