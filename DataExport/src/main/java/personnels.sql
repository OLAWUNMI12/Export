SELECT sp.ID,	sp.First_Name,	sp.Last_Name,	sp.Middle_Name,	sp.Phone,	sp.Email,	sp.Title,	sp.Staff_No,	sp.Type,	sp.Status,	sp.Send_Alert,
       sf.Name 'Facility',	so.Name AS 'Office',	sc.Name AS 'Client', sw.Name AS 'Workshop',	sp.Verification_Code,	sp.Verification_Code_Date
FROM  Sysdesk_Personnel AS sp
LEFT JOIN  Sysdesk_Office AS sf ON sf.ID = sp.Facility_Id
LEFT JOIN  Sysdesk_Office AS so ON so.ID = sp.Office_Id
LEFT JOIN  Sysdesk_Client AS sc ON sc.ID = sp.Client_Id
LEFT JOIN  Sysdesk_Workshop AS sw ON sw.ID = sp.Workshop_Id