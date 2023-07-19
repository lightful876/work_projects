SELECT 'New Repeat Customer' as Source,
NewRpt.custid, NewRpt.MostRecentAccount, Cust_DB1.Gender as Gender, 
    CAST(Cust_DB1.DateAcctOpen AS Date) as Open_Date, 
    Cust_DB1.Is_Cashloan_Acct as Is_CL, Cust_DB1.Is_HPorRF_Acct as Is_HPorRF, 
    Cust_DB1.Is_Both as Is_Both, Cust_DB1.Birthdate as Birthdate, 
    case  when Cust_DB1.Birthdate between '1997-1-1' and '2012-12-31' Then 'Gen Z'
    when Cust_DB1.Birthdate between '1981-1-1' and '1996-12-31' Then 'Millenial'
    when Cust_DB1.Birthdate between '1965-1-1' and '1980-12-31' Then 'Gen X'
    when Cust_DB1.Birthdate between '1955-1-1' and '1964-12-31' then 'Boomers 2'
    when Cust_DB1.Birthdate between '1946-1-1' and '1954-12-31' then 'Boomers 1'
    else 'Post-War' end as Age_Group, 
    case 
    when Left(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3) IN ('108', '112', '116', '144', '146', '147', '717', '718', '721', '186', '852', '191', '754', '756', '757', '199', '200', '873',
    '871', '861', '868', '869', '867', '865', '863', '879', '866', '451', '870', '862', '893', '872', '864', '883', '894', '874', '932', '933', '126', '127', '226') 
    then 'Courts ReadyCash'
    else
        case 
        when Left(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3) IN ('106', '113', '725', '114', '115', '143', '770', '771', '145', '148', '700', '701', '706', '711',
    '702', '719', '720', '190', '850', '858', '854', '857', '740', '743', '744', '745', '747', '749', '750', '960', '753', '720', '721', '722', '207',
    '612', '551', '553', '554', '557', '558', '560', '561', '562', '563', '565', '566', '617', '569', '571', '584', '588', '590', '815', '807', '812',
    '813', '710', '757', '758', '761', '762', '763', '767', '790', '791', '793', '794', '797', '900', '901', '903', '905', '907', '916', '931', '780', '784', '781', 
    '783', '786', '785', '778', '793', '782', '790', '811', '558', '566', '584', '560', '590', '106', '725', '960', '743', '740', '850', '858', '720', '900',
    '901', '903', '905', '907', '916', '936', '128', '129', '782', '600', '601', '602', '603', '604', '605','606', '607', '608', '609', '618', '630', '640', '650',
    '660', '670', '680', '881', '910', '920', '930', '940', '950', '970', '980', '990', '811', '814') 
    then 'Courts'
    else
        case
        when Left(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3) IN ('174', '173', '172', '177', '175', '615', '613', '171', '176', '614', '895', '878', '897', '600', '601', '604'
        , '605', '610', '612', '616', '742', '743', '750', '600', '741', '617') 
        then 'Lucky Dollar'
    else
        case
        when Left(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3) IN ('107', '111', '142', '714', '187', '192', '193', '194', '758', '204', '351', '341', '342', '346', '344', '203', '343', '201',
        '347', '350', '202', '345', '352', '351', '700', '706', '712', '703', '714', '717', '701', '711', '713', '707', '709', '704', '715', '771', '772', '773', '774', '775'
        ,'776', '777', '778', '919', '920', '930', '128', '129', '130', '717', '919', '920', '702', '714') 
        then 'Courts Optical' 
    else
        case
        when Left(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3) IN ('751', '876', '877', '875', '750', '752', '753', '782', '783', '921', '937', '921', '927', '781')
        then 'RadioShack'
    else
        CASE
        when Left(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3) IN ('922', '892', '805', '731')
        then 'Ashley'
    else
        CASE
        when Left(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3) IN ('121', '122', '123', '124')
        then 'Tropigas' 
    else 
        CASE
        WHEN LEFT(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3) IN ('125', '770', '349', '800')
        then 'Ecommerce'
    ELSE
        CASE
        WHEN LEFT(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3) in ('710', '790', '178', '816')
        then 'Telesales'
    ELSE
        CASE
        WHEN LEFT(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3)  in ('179', '183')
        then 'USA'
    END
    END
    END
    END
    END
    END
    END
    END
    END
    END AS Brand,
    case when LOWER(REPLACE(NewRpt.Country, ' ', '')) in ('antiguaandbarbuda', 'dominica', 
                                  'grenada', 'saintkitts', 'saintlucia', 
                                  'saintvincentandthegrenadines')
    then 'OECS' else NewRpt.Country end as Country, 
    DATEDIFF(year, NewRpt.DateSettled, Cust_DB1.DateAcctOpen) AS DateDelta,
    case when Cust_DB1.Is_Cashloan_Acct = 'Y' and Cust_DB1.Is_Both = 'N' Then 'CashLoan'
    when Cust_DB1.Is_HPorRF_Acct = 'Y' and Cust_DB1.Is_Both = 'N' Then 'HP or RF'
    When Cust_DB1.[Is_Both] = 'Y' Then 'Both' else 'Cash' end as acct_type
    FROM [CODS].[dbo].[Credit.CreditAccountSummaryFact] as NewRpt Inner Join [CODS].[dbo].[RPT_CL_HP_CUSTS_ACCT_SMRY] as Cust_DB1
    ON (NewRpt.MostRecentAccount = Cust_DB1.MostRecentAcctno)
    WHERE (DATEDIFF(year, NewRpt.DateSettled, Cust_DB1.DateAcctOpen)>= 5)
            AND (NewRpt.HPAccountACTIVE > 0 OR NewRpt.CreditAccountACTIVE > 0)
UNION ALL 
SELECT 'New New Customer' as Source, 
    NewNew.custid, NewNew.MostRecentAccount, Cust_DB2.Gender as Gender,
    CAST(Cust_DB2.DateAcctOpen AS Date) as Open_Date, 
    Cust_DB2.Is_Cashloan_Acct as Is_CL, 
    Cust_DB2.Is_HPorRF_Acct as Is_HPorRF, 
    Cust_DB2.Is_Both as Is_Both, Cust_DB2.Birthdate as Birthdate,
    case  when Cust_DB2.Birthdate between '1997-1-1' and '2012-12-31' Then 'Gen Z'
    when Cust_DB2.Birthdate between '1981-1-1' and '1996-12-31' Then 'Millenial'
    when Cust_DB2.Birthdate between '1965-1-1' and '1980-12-31' Then 'Gen X'
    when Cust_DB2.Birthdate between '1955-1-1' and '1964-12-31' then 'Boomers 2'
    when Cust_DB2.Birthdate between '1946-1-1' and '1954-12-31' then 'Boomers 1'
    else 'Post-War' end as Age_Group,
    case 
    when Left(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3) IN ('108', '112', '116', '144', '146', '147', '717', '718', '721', '186', '852', '191', '754', '756', '757', '199', '200', '873',
    '871', '861', '868', '869', '867', '865', '863', '879', '866', '451', '870', '862', '893', '872', '864', '883', '894', '874', '932', '933', '126', '127', '226') 
    then 'Courts ReadyCash'
    else
        case 
        when Left(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3) IN ('106', '113', '725', '114', '115', '143', '770', '771', '145', '148', '700', '701', '706', '711',
    '702', '719', '720', '190', '850', '858', '854', '857', '740', '743', '744', '745', '747', '749', '750', '960', '753', '720', '721', '722', '207',
    '612', '551', '553', '554', '557', '558', '560', '561', '562', '563', '565', '566', '617', '569', '571', '584', '588', '590', '815', '807', '812',
    '813', '710', '757', '758', '761', '762', '763', '767', '790', '791', '793', '794', '797', '900', '901', '903', '905', '907', '916', '931', '780', '784', '781', 
    '783', '786', '785', '778', '793', '782', '790', '811', '558', '566', '584', '560', '590', '106', '725', '960', '743', '740', '850', '858', '720', '900',
    '901', '903', '905', '907', '916', '936', '128', '129', '782', '600', '601', '602', '603', '604', '605','606', '607', '608', '609', '618', '630', '640', '650',
    '660', '670', '680', '881', '910', '920', '930', '940', '950', '970', '980', '990', '811', '814') 
    then 'Courts'
    else
        case
        when Left(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3) IN ('174', '173', '172', '177', '175', '615', '613', '171', '176', '614', '895', '878', '897', '600', '601', '604'
        , '605', '610', '612', '616', '742', '743', '750', '600', '741', '617') 
        then 'Lucky Dollar'
    else
        case
        when Left(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3) IN ('107', '111', '142', '714', '187', '192', '193', '194', '758', '204', '351', '341', '342', '346', '344', '203', '343', '201',
        '347', '350', '202', '345', '352', '351', '700', '706', '712', '703', '714', '717', '701', '711', '713', '707', '709', '704', '715', '771', '772', '773', '774', '775'
        ,'776', '777', '778', '919', '920', '930', '128', '129', '130', '717', '919', '920', '702', '714') 
        then 'Courts Optical' 
    else
        case
        when Left(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3) IN ('751', '876', '877', '875', '750', '752', '753', '782', '783', '921', '937', '921', '927', '781')
        then 'RadioShack'
    else
        CASE
        when Left(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3) IN ('922', '892', '805', '731')
        then 'Ashley'
    else
        CASE
        when Left(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3) IN ('121', '122', '123', '124')
        then 'Tropigas' 
    else 
        CASE
        WHEN LEFT(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3) IN ('125', '770', '349', '800')
        then 'Ecommerce'
    ELSE
        CASE
        WHEN LEFT(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3) in ('710', '790', '178', '816')
        then 'Telesales'
    ELSE
        CASE
        WHEN LEFT(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3)  in ('179', '183')
        then 'USA'
    END
    END
    END
    END
    END
    END
    END
    END
    END
    END AS Brand,
    case when LOWER(REPLACE(NewNew.Country, ' ', '')) in ('antiguaandbarbuda', 'dominica', 
                                  'grenada', 'saintkitts', 'saintlucia', 
                                  'saintvincentandthegrenadines')
    then 'OECS' else NewNew.Country end as Country, 
    DATEDIFF(year, DateSettled, Cust_DB2.DateAcctOpen) AS DateDelta,
    case when Cust_DB2.Is_Cashloan_Acct = 'Y' and Cust_DB2.Is_Both = 'N' Then 'CashLoan'
    when Cust_DB2.Is_HPorRF_Acct = 'Y' and Cust_DB2.Is_Both = 'N' Then 'HP or RF'
    When Cust_DB2.Is_Both = 'Y' Then 'Both' else 'Cash' end as acct_type
    FROM [CODS].[dbo].[Credit.CreditAccountSummaryFact] as NewNew 
          INNER JOIN [CODS].[dbo].[RPT_CL_HP_CUSTS_ACCT_SMRY] as Cust_DB2 
              ON NewNew.MostRecentAccount = Cust_DB2.MostRecentAcctno       
    WHERE NewNew.CreditAccountCOUNT = 1

/*UNION ALL 
SELECT 'New Cash Customer' as Source, NewCash.custid, NewCash.MostRecentAcctno, Cust_DB3.Gender as Gender,
    CAST(NewCash.DateAcctOpen AS Date) as Open_Date, 
    Cust_DB3.Is_Cashloan_Acct as Is_CL, Cust_DB3.Is_HPorRF_Acct as Is_HPorRF, 
    Cust_DB3.Is_Both as Is_Both, Cust_DB3.Birthdate as Birthdate,
    case  when Cust_DB3.Birthdate between '1997-1-1' and '2012-12-31' Then 'Gen Z'
    when Cust_DB3.Birthdate between '1981-1-1' and '1996-12-31' Then 'Millenial'
    when Cust_DB3.Birthdate between '1965-1-1' and '1980-12-31' Then 'Gen X'
    when cust_DB3.Birthdate between '1955-1-1' and '1964-12-31' then 'Boomers 2'
    when Cust_DB3.Birthdate between '1946-1-1' and '1954-12-31' then 'Boomers 1'
    else 'Post-War' end as Age_Group,
    case 
    when Left(CAST(Cust_DB3.MostRecentAcctno as varchar(max)), 3) IN ('108', '112', '116', '144', '146', '147', '717', '718', '721', '186', '852', '191', '754', '756', '757', '199', '200', '873',
    '871', '861', '868', '869', '867', '865', '863', '879', '866', '451', '870', '862', '893', '872', '864', '883', '894', '874', '932', '933', '126', '127', '226') 
    then 'Courts ReadyCash'
    else
        case 
        when Left(CAST(Cust_DB3.MostRecentAcctno as varchar(max)), 3) IN ('106', '113', '725', '114', '115', '143', '770', '771', '145', '148', '700', '701', '706', '711',
    '702', '719', '720', '190', '850', '858', '854', '857', '740', '743', '744', '745', '747', '749', '750', '960', '753', '720', '721', '722', '207',
    '612', '551', '553', '554', '557', '558', '560', '561', '562', '563', '565', '566', '617', '569', '571', '584', '588', '590', '815', '807', '812',
    '813', '710', '757', '758', '761', '762', '763', '767', '790', '791', '793', '794', '797', '900', '901', '903', '905', '907', '916', '931', '780', '784', '781', 
    '783', '786', '785', '778', '793', '782', '790', '811', '558', '566', '584', '560', '590', '106', '725', '960', '743', '740', '850', '858', '720', '900',
    '901', '903', '905', '907', '916', '936', '128', '129', '782', '600', '601', '602', '603', '604', '605','606', '607', '608', '609', '618', '630', '640', '650',
    '660', '670', '680', '881', '910', '920', '930', '940', '950', '970', '980', '990', '811', '814') 
    then 'Courts'
    else
        case
        when Left(CAST(Cust_DB3.MostRecentAcctno as varchar(max)), 3) IN ('174', '173', '172', '177', '175', '615', '613', '171', '176', '614', '895', '878', '897', '600', '601', '604'
        , '605', '610', '612', '616', '742', '743', '750', '600', '741', '617') 
        then 'Lucky Dollar'
    else
        case
        when Left(CAST(Cust_DB3.MostRecentAcctno as varchar(max)), 3) IN ('107', '111', '142', '714', '187', '192', '193', '194', '758', '204', '351', '341', '342', '346', '344', '203', '343', '201',
        '347', '350', '202', '345', '352', '351', '700', '706', '712', '703', '714', '717', '701', '711', '713', '707', '709', '704', '715', '771', '772', '773', '774', '775'
        ,'776', '777', '778', '919', '920', '930', '128', '129', '130', '717', '919', '920', '702', '714') 
        then 'Courts Optical' 
    else
        case
        when Left(CAST(Cust_DB3.MostRecentAcctno as varchar(max)), 3) IN ('751', '876', '877', '875', '750', '752', '753', '782', '783', '921', '937', '921', '927', '781')
        then 'RadioShack'
    else
        CASE
        when Left(CAST(Cust_DB3.MostRecentAcctno as varchar(max)), 3) IN ('922', '892', '805', '731')
        then 'Ashley'
    else
        CASE
        when Left(CAST(Cust_DB3.MostRecentAcctno as varchar(max)), 3) IN ('121', '122', '123', '124')
        then 'Tropigas' 
    else 
        CASE
        WHEN LEFT(CAST(Cust_DB3.MostRecentAcctno as varchar(max)), 3) IN ('125', '770', '349', '800')
        then 'Ecommerce'
    ELSE
        CASE
        WHEN LEFT(CAST(Cust_DB3.MostRecentAcctno as varchar(max)), 3) in ('710', '790', '178', '816')
        then 'Telesales'
    ELSE
        CASE
        WHEN LEFT(CAST(Cust_DB3.MostRecentAcctno as varchar(max)), 3)  in ('179', '183')
        then 'USA'
    END
    END
    END
    END
    END
    END
    END
    END
    END
    END AS Brand,
    case when LOWER(REPLACE(NewCash.Country, ' ', '')) in ('antiguaandbarbuda', 'dominica', 
                                  'grenada', 'saintkitts', 'saintlucia', 
                                  'saintvincentandthegrenadines')
    then 'OECS' else NewCash.Country end as Country, 
    DATEDIFF(year, SAccts.DateSettled, Cust_DB3.LatestsDateAcctOpen) AS DateDelta,
    case when Cust_DB3.Is_Cashloan_Acct = 'Y' and Cust_DB3.Is_Both = 'N' Then 'CashLoan'
    when Cust_DB3.Is_HPorRF_Acct = 'Y' and cust_DB3.Is_Both = 'N' Then 'HP or RF'
    When Cust_DB3.Is_Both = 'Y' Then 'Both' else 'Cash' end as acct_type    
    FROM [CODS].[dbo].[RPT_CASH_CUSTS_ACCT_SMRY] as NewCash
        Inner JOIN ([CODS].[dbo].[RPT_CL_HP_CUSTS_3YR_SUMMARY] as Cust_DB3             
 INNER JOIN
    [CODS].[dbo].[Credit.CreditAccountSummaryFact] as SAccts ON (SAccts.custid = Cust_DB3.custid)) ON (NewCash.custid = Cust_DB3.Custid)    
    WHERE NewCash.AgreementTotal > 0 AND DATEDIFF(year, SAccts.DateSettled, Cust_DB3.LatestsDateAcctOpen) >= 3
;*/

/*Separate Code for Total Customers*/
Select subquery.source, COUNT(subquery.custid) as cust_count, subquery.Fiscal, subquery.Country
From(
    SELECT 'New Cash' as source, NewCash.custid, CAST(NewCash.DateAcctOpen AS Date) as Open_Date, NewCash.Country, 
    case when NewCash.DateAcctOpen between '2019-04-01' and '2020-03-31' Then 'FY20'
    when NewCash.DateAcctOpen between '2020-04-01' and '2021-03-31' Then 'FY21'
    when NewCash.DateAcctOpen between '2021-04-01' and '2022-03-31' Then 'FY22'
    when NewCash.DateAcctOpen between '2022-04-01' and '2023-03-31' Then 'FY23'
    when NewCash.DateAcctOpen between '2023-04-01' and '2024-03-31' Then 'FY24'
    end as Fiscal
FROM [CODS].[dbo].[RPT_CASH_CUSTS_ACCT_SMRY] as NewCash) subquery
Group By subquery.source, subquery.Fiscal, subquery.Country
UNION ALL
SELECT subquery.source, COUNT(subquery.CustID) as cust_count, subquery.Fiscal, subquery.Country
FROM(
    SELECT 'New Credit' as source, NewCredit.CustID, CAST(NewCredit.LatestsDateAcctOpen AS Date) as Open_Date, NewCredit.Country,
case when NewCredit.LatestsDateAcctOpen between '2019-04-01' and '2020-03-31' Then 'FY20'
    when NewCredit.LatestsDateAcctOpen between '2020-04-01' and '2021-03-31' Then 'FY21'
    when NewCredit.LatestsDateAcctOpen between '2021-04-01' and '2022-03-31' Then 'FY22'
    when NewCredit.LatestsDateAcctOpen between '2022-04-01' and '2023-03-31' Then 'FY23'
    when NewCredit.LatestsDateAcctOpen between '2023-04-01' and '2024-03-31' Then 'FY24'
    end as Fiscal
FROM [CODS].[dbo].[RPT_CL_HP_CUSTS_3YR_SUMMARY] as NewCredit
) subquery
Group By subquery.source, subquery.Fiscal, subquery.Country;


/*Inner JOIN [CODS].[dbo].[RPT_CL_HP_CUSTS_3YR_SUMMARY] as Cust_DB3 ON NewCash.custid = Cust_DB3.Custid
WHERE NewCash.AgreementTotal > 0 AND Is_Cancelled = 'N' AND DATEDIFF(year, DateAcctOpen, GETDATE()) <= 5*/