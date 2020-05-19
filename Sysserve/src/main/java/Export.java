import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class Export {
    private static final String url ="jdbc:mysql://testserver.sysservesolutions.com/test_fleet" ; //"jdbc:mysql://server1.sysservesolutions.com/ci_sysdesk"
    private static final String username ="test_fleet"; //"ci_sysdesk";
    private static final String password =   "test_fleet123";// "4ZVFddHcj";
    private Connection getConnection() throws ClassNotFoundException, SQLException {
        Connection connection;
        Class.forName("com.mysql.jdbc.Driver");
        connection = DriverManager.getConnection(this.url,this.username,this.password);
        return connection;
    }

    private ArrayList<String> getTables() throws SQLException, ClassNotFoundException {
        ArrayList<String> databaseTables = new ArrayList<String>();
            Connection connection = getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            String[] types = {"TABLE"};
            ResultSet tables = metaData.getTables(null, null, "%", types);
            while (tables.next()) {
                System.out.println(tables.getString("TABLE_NAME"));
                databaseTables.add(tables.getString("TABLE_NAME"));
            }
        if(connection != null){
            connection.close();
        }
        return databaseTables;
    }

    private ArrayList<String> getColumns(String tableName) throws SQLException, ClassNotFoundException {
        ArrayList<String> tableColumns = new ArrayList<String>();
        Connection connection = getConnection();
        Statement stmt = null;
        String query = "select * from " + tableName;
        stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        ResultSetMetaData rsMetaData = rs.getMetaData();
        int numberOfColumns = rsMetaData.getColumnCount();
        for (int i = 1; i < numberOfColumns + 1; i++) {
            tableColumns.add(rsMetaData.getColumnName(i));
        }
        if(connection != null){
            connection.close();
        }
        return tableColumns;
    }

    private void filterReferences(String tableName,ArrayList<String> columnNames,ArrayList<String> reference,HashMap<String,String> references){
        if(!tableName.contains("inventory")){
            references.remove("Inventory_Category_Id");
            references.remove("Inventory_SubCategory_Id");
            references.remove("Inventory_Department_Id");
        }else {
            references.remove("Category_Id");
            references.remove("SubCategory_Id");
            references.remove("Department_Id");
        }

        if(columnNames.contains("Apartment_Id")){
            reference.remove("Facility_Apartment_Id");
            references.remove("Facility_Apartment_Id");
        }
        if(columnNames.contains("Facility_Apartment_Id")){
            reference.remove("Apartment_Id");
            references.remove("Apartment_Id");
        }

        if(columnNames.contains("Apartment_Type_Id")){
            reference.remove("Facility_Apartment_Type_Id");
            references.remove("Facility_Apartment_Type_Id");
        }
        if(columnNames.contains("Facility_Apartment_Type_Id")){
            reference.remove("Apartment_Type_Id");
            references.remove("Apartment_Type_Id");
        }

        if(columnNames.contains("Facility_Maintenance_Type_Id")){
            reference.remove("Maintenance_Type_Id");
            references.remove("Maintenance_Type_Id");
        }

        if(columnNames.contains("Maintenance_Type_Id")){
            reference.remove("Facility_Maintenance_Type_Id");
            references.remove("Facility_Maintenance_Type_Id");
            reference.remove("Vehicle_Maintenance_Type_Id");
            references.remove("Vehicle_Maintenance_Type_Id");
        }

        if(columnNames.contains("Facility_Maintenance_Group_Id")){
            reference.remove("Maintenance_Group_Id");
            references.remove("Maintenance_Group_Id");
        }

        if(columnNames.contains("Maintenance_Group_Id")){
            reference.remove("Facility_Maintenance_Group_Id");
            references.remove("Facility_Maintenance_Group_Id");
        }

        if(columnNames.contains("Vehicle_Maintenance_Type_Id")){
            reference.remove("Maintenance_Type_Id");
            references.remove("Maintenance_Type_Id");
        }

        if(columnNames.contains("Vehicle_Maintenance_Category_Id")){
            reference.remove("Category_Id");
        }
    }

    private HashMap<String,String> findColumnToReference(ArrayList<String> columnNames,ArrayList<String> reference, String tableName){
        HashMap<String,String> columnToReference = new HashMap<String, String>();
        for (int k = 0; k < columnNames.size(); k ++){
            String column = columnNames.get(k);
            for(int a = 0 ; a < reference.size(); a++){
                if(((column.contains("_" + reference.get(a)) && !(((column.equals("Category_Id") && reference.get(a).equals("Vehicle_Category_Id")) || ((column.equals("Vehicle_Category_Id") && reference.get(a).equals("Category_Id")))) || (((column.equals("Location_Id") && reference.get(a).equals("Office_Location_Id"))) || ((column.equals("Office_Location_Id") && reference.get(a).equals("Location_Id"))))))  || (column.equals(reference.get(a))  && !(column.equals("Category_Id") && tableName.equals("sysdesk_inventory_manufacturer"))))){
                    if(columnToReference.containsKey(reference.get(a))){
                        columnToReference.put(reference.get(a)+a,column);
                    }else {
                        columnToReference.put(reference.get(a),column);
                    }

                }
            }
        }
    return columnToReference;
    }

    private String buildQuery(String tableName,HashMap<String,String> columnToReference, ArrayList<String> columnNames, ArrayList<String> referencedColumnNames,ArrayList<String> reference, HashMap<String,String> references,ArrayList<String> tableNames){
        boolean addReference = false;
        String refQuery = "";
        String joinQuery = "";
        if(columnToReference != null && columnToReference.size() > 0){
            if(columnNames  != null && columnNames.size() > 0 ){
                for(int count = 0; count < columnNames.size(); count++){
                    if(count == columnNames.size() - 1){
                        refQuery += "tb." + columnNames.get(count) + " ";
                    }else {
                        refQuery += "tb." + columnNames.get(count) + " , ";
                    }
                }
            }

        for(int counter = 0; counter < reference.size(); counter++){
            String replacedQuery = "";
            String key = "";
            Iterator iterator = columnToReference.entrySet().iterator();
            int constraint = 0;
            while (iterator.hasNext()){
                Map.Entry entry = (Map.Entry) iterator.next();
                key = (String) entry.getKey();
                if(((key.contains("_" + reference.get(counter)) && !(((key.equals("Category_Id") && reference.get(counter).equals("Vehicle_Category_Id")) || ((key.equals("Vehicle_Category_Id") && reference.get(counter).equals("Category_Id")))) || (((key.equals("Location_Id") && reference.get(counter).equals("Office_Location_Id"))) || ((key.equals("Office_Location_Id") && reference.get(counter).equals("Location_Id"))))))  || key.equals(reference.get(counter)))){
                    constraint++;
                    String replaceColumn =  columnToReference.get(key);
                    String referenceTable =  references.get(reference.get(counter));
                    if(tableName.contains("inventory")&& (replaceColumn.equals("Category_Id") || replaceColumn.equals("SubCategory_Id") || replaceColumn.equals("Department_Id") )){
                        referenceTable =  references.get("Inventory_" +reference.get(counter));
                    }
                    if(tableName.contains("helpdesk")&& (replaceColumn.equals("Category_Id") || replaceColumn.equals("Urgency_Id") || replaceColumn.equals("Medium_Id") )){
                        referenceTable =  references.get("Helpdesk_" +reference.get(counter));
                    }
                    if(tableNames.contains(referenceTable)){
                        if(reference.get(counter).equals("Personnel_Id") || reference.get(counter).equals("Driver_Id")){
                            if(constraint == 1){
                                replacedQuery =  refQuery.replace("tb." + replaceColumn,  "CONCAT(" + "tb" + counter + ".First_Name,' '," +  "tb" + counter + ".Last_Name) AS " + replaceColumn + counter);
                            }else {
                                replacedQuery =  refQuery.replace("tb." + replaceColumn,  "CONCAT(" + "tb" + counter + constraint + ".First_Name,' '," +  "tb" + counter +  constraint + ".Last_Name) AS " + replaceColumn + counter + constraint );
                            }
                        }else if(reference.get(counter).equals("Category_Id") || reference.get(counter).equals("SubCategory_Id")){
                            if(tableName.contains("inventory")){
                                if(constraint == 1){
                                    replacedQuery = refQuery.replace("tb." + replaceColumn, "tb" + counter + ".Name AS " + replaceColumn + counter);
                                }else {
                                    replacedQuery = refQuery.replace("tb." + replaceColumn, "tb" + counter + constraint + ".Name AS " + replaceColumn + counter + constraint);
                                }
                            }else{
                                if(constraint == 1){
                                    replacedQuery = refQuery.replace("tb." + replaceColumn, "tb" + counter + ".Description AS " + replaceColumn + counter);
                                }else {
                                    replacedQuery = refQuery.replace("tb." + replaceColumn, "tb" + counter + constraint + ".Description AS " + replaceColumn + counter + constraint);
                                }
                            }
                        }else if( reference.get(counter).equals("Closed_Pending_Type_Id") || reference.get(counter).equals("Stake_Holder_Id") || reference.get(counter).equals("Action_Id") || reference.get(counter).equals("Status_Id") || reference.get(counter).equals("CheckList_Type_Item_Id") || reference.get(counter).equals("Facility_Generator_Log_Id") || reference.get(counter).equals("Facility_Maintenance_Type_Id") || reference.get(counter).equals("Maintenance_Type_Id") || reference.get(counter).equals("Facility_Maintenance_Group_Id") || reference.get(counter).equals("Maintenance_Group_Id") || reference.get(counter).equals("Facility_Log_Id") || reference.get(counter).equals("Purchase_Order_Item_Id") || reference.get(counter).equals("Urgency_Id") || reference.get(counter).equals("Vehicle_Maintenance_Category_Id")  || reference.get(counter).equals("Medium_Id")){
                            if(constraint == 1){
                                replacedQuery = refQuery.replace("tb." + replaceColumn, "tb" + counter + ".Description AS " + replaceColumn + counter);
                            }else {
                                replacedQuery = refQuery.replace("tb." + replaceColumn, "tb" + counter + constraint + ".Description AS " + replaceColumn + counter + constraint);
                            }
                        } else if(reference.get(counter).equals("Bank_Account_Id") || reference.get(counter).equals("BankAccount_Id") ){
                            if(constraint == 1){
                                replacedQuery =  refQuery.replace("tb." + replaceColumn,  "CONCAT(" + "tb" + counter + ".Bank_Name,' '," +  "tb" + counter + ".Number) AS " + replaceColumn + counter);
                            }else {
                                replacedQuery =  refQuery.replace("tb." + replaceColumn,  "CONCAT(" + "tb" + counter + constraint +  ".Bank_Name,' '," +  "tb" + counter + constraint + ".Number) AS " + replaceColumn + counter + constraint);
                            }
                        }
                        else if(reference.get(counter).equals("Facility_Apartment_Id") || reference.get(counter).equals("Apartment_Id")) {
                            if (constraint == 1) {
                                replacedQuery = refQuery.replace("tb." + replaceColumn, "CONCAT(" + "tb" + counter + ".Description,' '," + "tb" + counter + ".Apartment_No) AS " + replaceColumn + counter);
                            } else {
                                replacedQuery = refQuery.replace("tb." + replaceColumn, "CONCAT(" + "tb" + counter + constraint + ".Description,' '," + "tb" + counter + constraint + ".Apartment_No) AS " + replaceColumn + counter + constraint);
                            }
                        }else if(reference.get(counter).equals("Vehicle_Group_Id")){
                            if(constraint == 1){
                                replacedQuery =  refQuery.replace("tb." + replaceColumn,  "CONCAT(" + "tb" + counter + ".Make,' '," +  "tb" + counter + ".Model"  + ") AS " + replaceColumn + counter);
                            }else{
                                replacedQuery =  refQuery.replace("tb." + replaceColumn,  "CONCAT(" + "tb" + counter + constraint + ".Make,' '," +  "tb" + counter + constraint + ".Model"  + ") AS " + replaceColumn + counter + constraint);
                            }
                        }else if(reference.get(counter).equals("Vehicle_Id")){
                        if(constraint == 1){
                            replacedQuery =  refQuery.replace("tb." + replaceColumn,  "CONCAT(" + "tb" + counter + ".Make,' '," +  "tb" + counter + ".Model" + ",' '," +"tb" + counter + ".Plate_Number" + ") AS " + replaceColumn + counter);
                        }else{
                            replacedQuery =  refQuery.replace("tb." + replaceColumn,  "CONCAT(" + "tb" + counter + constraint + ".Make,' '," +  "tb" + counter + constraint + ".Model" + ",' '," +"tb" + counter + constraint + ".Plate_Number" + ") AS " + replaceColumn + counter + constraint);
                            }
                        }else if(reference.get(counter).equals("Issue_Id") || reference.get(counter).equals("Bulk_Notification_Id") ) {
                            if(constraint == 1){
                                replacedQuery = refQuery.replace("tb." + replaceColumn, "tb" + counter + ".Title AS " + replaceColumn + counter);
                            }else {
                                replacedQuery = refQuery.replace("tb." + replaceColumn, "tb" + counter + constraint + ".Title AS " + replaceColumn + counter + constraint);
                            }
                        }else if(reference.get(counter).equals("Facility_Booking_Id")) {
                            if(constraint == 1){
                                replacedQuery = refQuery.replace("tb." + replaceColumn, "tb" + counter + ".Remark AS " + replaceColumn + counter);
                            }else {
                                replacedQuery = refQuery.replace("tb." + replaceColumn, "tb" + counter + constraint + ".Remark AS " + replaceColumn + counter + constraint);
                            }
                        }else if(reference.get(counter).equals("Tonnage_Id")) {
                            if(constraint == 1){
                                replacedQuery = refQuery.replace("tb." + replaceColumn, "tb" + counter + ".Tonnage AS " + replaceColumn + counter);
                            }else {
                                replacedQuery = refQuery.replace("tb." + replaceColumn, "tb" + counter + constraint + ".Tonnage AS " + replaceColumn + counter + constraint);
                            }
                        }else if(reference.get(counter).equals("Purchase_Order_Id")) {
                            if(constraint == 1){
                                replacedQuery = refQuery.replace("tb." + replaceColumn, "tb" + counter + ".Number AS " + replaceColumn + counter);
                            }else {
                                replacedQuery = refQuery.replace("tb." + replaceColumn, "tb" + counter + constraint + ".Number AS " + replaceColumn + counter + constraint);
                            }
                        }else{
                            if(constraint == 1){
                                replacedQuery = refQuery.replace("tb." + replaceColumn, "tb" + counter + ".Name AS " + replaceColumn + counter);
                            }else {
                                replacedQuery = refQuery.replace("tb." + replaceColumn, "tb" + counter + constraint + ".Name AS " + replaceColumn + counter + constraint);
                            }
                        }

                        if(constraint == 1){
                            joinQuery += "LEFT JOIN " + referenceTable + " AS tb" + counter + " ON " + "tb." + replaceColumn  + " = " + "tb" + counter + ".ID ";

                        }else {
                            joinQuery += "LEFT JOIN " + referenceTable + " AS tb" + counter + constraint + " ON " + "tb." + replaceColumn + " = " + "tb" + counter + constraint + ".ID ";
                        }

                        if(constraint == 1){
                            int num = columnNames.indexOf(replaceColumn);
                            referencedColumnNames.set(num,replaceColumn + counter);
                        }else {
                            int num = columnNames.indexOf(replaceColumn);
                            referencedColumnNames.set(num,replaceColumn + counter + constraint);
                        }
                    }

                }
                if(!replacedQuery.equals("")){
                    refQuery = replacedQuery;
                }
            }
        }
            if(!refQuery.equals("") && !joinQuery.equals("")){
                addReference = true;
            }
        }
        String query = "SELECT * FROM " + tableName;
        if(addReference){
            query += " AS tb ";
            String fullQuery = query.replace("*" ,refQuery);
            fullQuery += joinQuery;
            query = fullQuery;
        }
    return query;
    }

    private void exportToFile(String parentFileDir,String tableName, ArrayList<String> referencedColumnNames , ArrayList<String> columnNames, String query) throws FileNotFoundException, SQLException, ClassNotFoundException {
        File file = new File(parentFileDir + "\\" + tableName + ".csv");
        PrintWriter printWriter = new PrintWriter(file);
        StringBuilder stringBuilder = new StringBuilder();
        for (int col = 0  ; col < columnNames.size() ; col++) {
            stringBuilder.append(columnNames.get(col).trim());
            stringBuilder.append("\t");
        }
        stringBuilder.append("\n");

        Connection connection = getConnection();
        Statement stmt;
        stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
            for(int j =0; j < referencedColumnNames.size(); j++){
                if((referencedColumnNames.get(j).trim().endsWith("_Id")  || referencedColumnNames.get(j).trim().equals("Version") || referencedColumnNames.get(j).trim().equals("Max_Uses") || referencedColumnNames.get(j).trim().equals("Max_Uses_User") || referencedColumnNames.get(j).trim().equals("Item_No") || referencedColumnNames.get(j).trim().equals("Frequency_Value") || referencedColumnNames.get(j).trim().equals("Period_No")) &&
                        (!referencedColumnNames.get(j).trim().equals("User_Id") && !referencedColumnNames.get(j).trim().equals("Device_Id") && !referencedColumnNames.get(j).trim().equals("App_Tracker_Id") && !referencedColumnNames.get(j).trim().equals("Tracker_Id") && !referencedColumnNames.get(j).trim().equals("Affiliate_Id") && !referencedColumnNames.get(j).trim().equals("Requisition_Payee_Id") && !(referencedColumnNames.get(j).trim().equals("Category_Id") && tableName.equals("sysdesk_inventory_manufacturer"))) ){
                    stringBuilder.append(rs.getInt(referencedColumnNames.get(j)));
                }else if((referencedColumnNames.get(j).trim().endsWith("Date") || referencedColumnNames.get(j).trim().endsWith("Time")) && !(referencedColumnNames.get(j).trim().equals("Has_Expiry_Date"))){
                    stringBuilder.append(rs.getTimestamp(referencedColumnNames.get(j)));
                }else if((referencedColumnNames.get(j).trim().equals("Value") && tableName.equals("Helpdesk_Table_Setting")) || referencedColumnNames.get(j).trim().equals("Withholding_Percentage") || referencedColumnNames.get(j).trim().equals("Tonnage") || referencedColumnNames.get(j).trim().equals("Mileage") || referencedColumnNames.get(j).trim().contains("Latitude") || referencedColumnNames.get(j).trim().contains("Longitude") || referencedColumnNames.get(j).trim().equals("Cost") ||(referencedColumnNames.get(j).trim().contains("Amount") && !(referencedColumnNames.get(j).trim().contains("Fixed_Amount") && tableName.equals("Sysdesk_Discount_Code")))  ||  referencedColumnNames.get(j).trim().contains("Price") || referencedColumnNames.get(j).trim().equals("Quantity") || referencedColumnNames.get(j).trim().equals("Exchange_Rate") || referencedColumnNames.get(j).trim().contains("_Hour") || referencedColumnNames.get(j).trim().contains("_Stock")){
                    stringBuilder.append(rs.getDouble(referencedColumnNames.get(j)));
                }else{
                    stringBuilder.append(rs.getString(referencedColumnNames.get(j)));
                }
                stringBuilder.append("\t");
            }
            stringBuilder.append("\n");
        }
        printWriter.write(stringBuilder.toString());
        printWriter.close();
        if(connection != null){
            connection.close();
        }
    }

    private void zipExportFolder(File sourceFolder, File destinationFolder) throws IOException {
        ZipOutputStream zipOutputStream = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(destinationFolder)));
        BufferedInputStream bufferedInputStream = null;
        byte[] data = new byte[1024];
        String[] filesNames = sourceFolder.list();
        for (int i = 0; i < filesNames.length; i++ ){
            bufferedInputStream = new BufferedInputStream(new FileInputStream(sourceFolder.getPath() + "/" + filesNames[i]),1024);
            zipOutputStream.putNextEntry(new ZipEntry(filesNames[i]));
            int count;
            while((count = bufferedInputStream.read(data,0,1000)) != -1)
            {
                zipOutputStream.write(data, 0, count);
            }
            zipOutputStream.closeEntry();
        }
        zipOutputStream.flush();
        zipOutputStream.close();
    }


    private void exportData() throws SQLException, ClassNotFoundException, IOException {
        HashMap<String,String> databaseReferences = new HashMap<String, String>();
        databaseReferences.put("Client_Id","Sysdesk_Client");
        databaseReferences.put("Office_Id","Sysdesk_Office");
        databaseReferences.put("Vehicle_Group_Id","Sysdesk_Vehicle_Group");
        databaseReferences.put("Vehicle_PPM_Group_Id","Sysdesk_Vehicle_PPM_Group");
        databaseReferences.put("Vehicle_Category_Id","Sysdesk_Vehicle_Category");
        databaseReferences.put("Vehicle_Maintenance_Type_Id","Sysdesk_Vehicle_Maintenance_Type");
        databaseReferences.put("Vehicle_Maintenance_Category_Id","Sysdesk_Vehicle_Maintenance_Category");
        databaseReferences.put("Personnel_Id","Sysdesk_Personnel");
        databaseReferences.put("Driver_Id","Sysdesk_Personnel");
        databaseReferences.put("Category_Id","Sysdesk_Category");
        databaseReferences.put("Inventory_Category_Id","Sysdesk_Inventory_Category");
        databaseReferences.put("SubCategory_Id","Sysdesk_SubCategory");
        databaseReferences.put("Inventory_SubCategory_Id","Sysdesk_Inventory_SubCategory");
        databaseReferences.put("Helpdesk_Category_Id","Helpdesk_Category");
        databaseReferences.put("Helpdesk_SubCategory_Id","Helpdesk_SubCategory");
        databaseReferences.put("Workshop_Id","Sysdesk_Workshop");
        databaseReferences.put("Facility_Id","Sysdesk_Facility");
        databaseReferences.put("Facility_Group_Id","Sysdesk_Facility_Group");
        databaseReferences.put("Facility_Apartment_Id","Sysdesk_Facility_Apartment");
        databaseReferences.put("Apartment_Id","Sysdesk_Facility_Apartment");
        databaseReferences.put("Facility_Apartment_Occupant_Id","Sysdesk_Facility_Apartment_Occupant");
        databaseReferences.put("Facility_Apartment_Type_Id","Sysdesk_Facility_Apartment_Type");
        databaseReferences.put("Apartment_Type_Id","Sysdesk_Facility_Apartment_Type");
        databaseReferences.put("Facility_Apartment_Group_Id","Sysdesk_Facility_Apartment_Group");
        databaseReferences.put("Facility_Booking_Id","Sysdesk_Facility_Booking");
        databaseReferences.put("Facility_Generator_Log_Id","Sysdesk_Facility_Generator_Log");
        databaseReferences.put("Facility_Maintenance_Type_Id","Sysdesk_Facility_Maintenance_Type");
        databaseReferences.put("Maintenance_Type_Id","Sysdesk_Facility_Maintenance_Type");
        databaseReferences.put("Facility_Maintenance_Group_Id","Sysdesk_Facility_Maintenance_Group");
        databaseReferences.put("Maintenance_Group_Id","Sysdesk_Facility_Maintenance_Group");
        databaseReferences.put("Facility_Lease_Fee_Type_Id","Sysdesk_Facility_Lease_Fee_Type");
        databaseReferences.put("Facility_Log_Id","Sysdesk_Facility_Log");
        databaseReferences.put("Filling_Station_Id","Sysdesk_Filling_Station");
        databaseReferences.put("Vendor_Id","Sysdesk_Vendor");
        databaseReferences.put("Vehicle_Id","Sysdesk_Vehicle");
        databaseReferences.put("Bank_Account_Id","Sysdesk_Account");
        databaseReferences.put("BankAccount_Id","Sysdesk_Account");
        databaseReferences.put("Warehouse_Id","Sysdesk_Inventory_Warehouse");
        databaseReferences.put("Document_Type_Id","Sysdesk_Document_Type");
        databaseReferences.put("Issue_Id","HelpDesk_Issues");
        databaseReferences.put("Action_Id","HelpDesk_Issue_Action");
        databaseReferences.put("Closed_Pending_Type_Id","HelpDesk_Closed_Pending_Types");
        databaseReferences.put("Stake_Holder_Id","HelpDesk_Stake_Holder");
        databaseReferences.put("Status_Id","HelpDesk_Status");
        databaseReferences.put("Bulk_Notification_Id","Sysdesk_Bulk_Notification");
        databaseReferences.put("CheckList_Type_Id","Sysdesk_CheckList_Type");
        databaseReferences.put("CheckList_Type_Item_Id","Sysdesk_CheckListType_Item");
        databaseReferences.put("Customer_Id","Sysdesk_Customer");
        databaseReferences.put("Department_Id","Sysdesk_Department");
        databaseReferences.put("Inventory_Department_Id","Sysdesk_Inventory_Department");
        databaseReferences.put("Discount_Code_Id","Sysdesk_Discount_Code");
        databaseReferences.put("Landlord_Id","Sysdesk_Landlord");
        databaseReferences.put("Agency_Id","Sysdesk_Agency");
        databaseReferences.put("Solicitor_Id","Sysdesk_Solicitor");
        databaseReferences.put("Purchase_Order_Id","Sysdesk_Inventory_Purchase_Order");
        databaseReferences.put("Purchase_Order_Item_Id","Sysdesk_Inventory_Purchase_Order_Item");
        databaseReferences.put("Model_Id","Sysdesk_Inventory_Model");
        databaseReferences.put("Specification_Id","Sysdesk_Inventory_Specification");
        databaseReferences.put("Supplier_Id","Sysdesk_Inventory_Supplier");
        databaseReferences.put("Manufacturer_Id","Sysdesk_Inventory_Manufacturer");
        databaseReferences.put("Office_Location_Id","Sysdesk_Office_Location");
        databaseReferences.put("Location_Id","Sysdesk_Location");
        databaseReferences.put("Pickup_Id","Sysdesk_Location");
        databaseReferences.put("Destination_Id","Sysdesk_Location");
        databaseReferences.put("Service_Charge_Setting_Id","Sysdesk_Service_Charge_Setting");
        databaseReferences.put("Tonnage_Id","Sysdesk_Tonnage");
        databaseReferences.put("Urgency_Id","Sysdesk_Urgency");
        databaseReferences.put("Helpdesk_Urgency_Id","Helpdesk_Urgency");
        databaseReferences.put("Medium_Id","Sysdesk_Medium");
        databaseReferences.put("Helpdesk_Medium_Id","Helpdesk_Medium");

        ArrayList<String> databaseReference = new ArrayList<String>();
        databaseReference.add("Client_Id");
        databaseReference.add("Office_Id");
        databaseReference.add("Personnel_Id");
        databaseReference.add("Driver_Id");
        databaseReference.add("Category_Id");
        databaseReference.add("SubCategory_Id");
        databaseReference.add("Workshop_Id");
        databaseReference.add("Facility_Id");
        databaseReference.add("Vehicle_Group_Id");
        databaseReference.add("Vehicle_PPM_Group_Id");
        databaseReference.add("Vehicle_Category_Id");
        databaseReference.add("Vehicle_Maintenance_Type_Id");
        databaseReference.add("Vehicle_Maintenance_Category_Id");
        databaseReference.add("Facility_Group_Id");
        databaseReference.add("Apartment_Id");
        databaseReference.add("Facility_Apartment_Id");
        databaseReference.add("Facility_Apartment_Occupant_Id");
        databaseReference.add("Facility_Apartment_Type_Id");
        databaseReference.add("Apartment_Type_Id");
        databaseReference.add("Facility_Apartment_Group_Id");
        databaseReference.add("Facility_Booking_Id");
        databaseReference.add("Facility_Maintenance_Type_Id");
        databaseReference.add("Maintenance_Type_Id");
        databaseReference.add("Facility_Maintenance_Group_Id");
        databaseReference.add("Maintenance_Group_Id");
        databaseReference.add("Facility_Generator_Log_Id");
        databaseReference.add("Facility_Lease_Fee_Type_Id");
        databaseReference.add("Facility_Log_Id");
        databaseReference.add("Filling_Station_Id");
        databaseReference.add("Vendor_Id");
        databaseReference.add("Vehicle_Id");
        databaseReference.add("Bank_Account_Id");
        databaseReference.add("BankAccount_Id");
        databaseReference.add("Warehouse_Id");
        databaseReference.add("Document_Type_Id");
        databaseReference.add("Issue_Id");
        databaseReference.add("Action_Id");
        databaseReference.add("Closed_Pending_Type_Id");
        databaseReference.add("Stake_Holder_Id");
        databaseReference.add("Status_Id");
        databaseReference.add("Bulk_Notification_Id");
        databaseReference.add("CheckList_Type_Id");
        databaseReference.add("CheckList_Type_Item_Id");
        databaseReference.add("Customer_Id");
        databaseReference.add("Department_Id");
        databaseReference.add("Discount_Code_Id");
        databaseReference.add("Landlord_Id");
        databaseReference.add("Agency_Id");
        databaseReference.add("Solicitor_Id");
        databaseReference.add("Purchase_Order_Id");
        databaseReference.add("Purchase_Order_Item_Id");
        databaseReference.add("Model_Id");
        databaseReference.add("Specification_Id");
        databaseReference.add("Supplier_Id");
        databaseReference.add("Manufacturer_Id");
        databaseReference.add("Location_Id");
        databaseReference.add("Office_Location_Id");
        databaseReference.add("Pickup_Id");
        databaseReference.add("Destination_Id");
        databaseReference.add("Service_Charge_Setting_Id");
        databaseReference.add("Tonnage_Id");
        databaseReference.add("Urgency_Id");
        databaseReference.add("Medium_Id");

        ArrayList<String> tableNames = getTables();
        File parentFile = new File("C:/Users/MASON/Desktop/Test");
        parentFile.mkdir();
        if(tableNames != null){
            for(int i =0; i < tableNames.size(); i++){
                ArrayList<String> reference = new ArrayList<String>();
                reference.addAll(databaseReference);
                HashMap<String,String> references = new HashMap<String, String>();
                references.putAll(databaseReferences);

                System.out.println(tableNames.get(i));
                System.out.println(i);

                ArrayList<String> columnNames  = getColumns(tableNames.get(i));
                ArrayList<String> referencedColumnNames = new ArrayList<String>();
                referencedColumnNames.addAll(columnNames);

                filterReferences(tableNames.get(i),columnNames,reference,references);

                HashMap<String,String> columnToReference = findColumnToReference(columnNames,reference,tableNames.get(i));

                String query = buildQuery(tableNames.get(i),columnToReference,columnNames,referencedColumnNames,reference,references,tableNames);

                exportToFile(parentFile.getAbsolutePath(),tableNames.get(i),referencedColumnNames,columnNames,query);
            }
        }
      zipExportFolder(parentFile,new File(parentFile.getAbsolutePath() + ".zip"));
    }


    public static void main(String[] arg) throws IOException, SQLException, ClassNotFoundException {
//        Export export = new Export();
//        export.exportData();
       System.out.println(System.getProperty("java.io.tmpdir")+ File.separatorChar + "export" + File.separatorChar+"ugdiuwg");
        System.out.println(System.getProperty("os.name"));



//        try {
//            Path path = Files.createTempDirectory("Folders");
////            path.toFile().renameTo(new File("Folders"));
////            File file = File.createTempFile("File",".txt",path.toFile());
//            File file = new File(System.getProperty("java.io.tmpdir") + "Folders");
//            file.mkdir();
//            System.out.println(System.getProperty("java.io.tmpdir"));
//            System.out.println(file.getAbsolutePath());
//            File file2 = new File(file.getAbsolutePath() + "\\Fold.csv");
//            System.out.println(file2.getAbsolutePath());
//            System.out.println(file2.getName());
//            System.out.println(path.toAbsolutePath().toString());
//            PrintWriter printWriter = new PrintWriter(file2);
//            printWriter.write("hey");
//            printWriter.close();
//
//            for(int i = 0 ;i <file.listFiles().length; i++){
//                System.out.println(file.listFiles()[i].getName());
//            }
//            export.zipExportFolder(file,new File(file.getAbsolutePath() + ".zip"));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }
}
