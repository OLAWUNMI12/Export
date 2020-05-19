SELECT sv.ID,	sv.Version, sv.Code, sv.Type,	sv.Make, sv.Model,	sv.Year, sv.Colour, sv.Plate_Number,	sv.Status, sv.Send_Alert,
       sv.Capacity,	sc.Name AS 'Client',	CONCAT(sp.First_Name,' ',sp.Last_Name) AS 'Driver', CONCAT(sp2.First_Name,' ',sp2.Last_Name) AS 'Co-Driver',
       so.Name AS 'Office', sv.Location,	sv.Chassis_No, svg.Name AS 'Vehicle group',	sv.Engine_No,	sv.Sub_Status, sv.Current_Location,	sv.Latitude,sv.Longitude,
       sv.Mileage,	sv.Purchased_Date,sv.Battery_Specification,	sv.Tyre_Specification, 	sv.Mileage_Date,	sv.Tracker_Id AS 'Tracker',	sv.App_Tracker_Id AS 'App tracker', sv.Tracker_Latitude,
       sv.Tracker_Longitude, sv.App_Tracker_Latitude, sv.App_Tracker_Longitude,	sv.Transmission_Type,	sv.Purchased_Amount,
       sv.Supplier,sv.Assigned_To_User_Name,	sv.Vehicle_Specification
FROM Sysdesk_Vehicle AS sv
LEFT JOIN Sysdesk_Client As sc ON sc.ID = sv.Client_Id
LEFT JOIN Sysdesk_Personnel As sp ON sp.ID = sv.Driver_Personnel_Id
LEFT JOIN Sysdesk_Personnel As sp2 ON sp2.ID = sv.Driver_Personnel_Id
LEFT JOIN Sysdesk_Office As so ON so.ID = sv.Office_Id
LEFT JOIN Sysdesk_Vehicle_Group As svg ON svg.ID = sv.Vehicle_Group_Id

