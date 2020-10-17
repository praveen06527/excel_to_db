/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dbutils.exceltodb;

import com.db.DBConnection;
import com.db.DBDMLStatement;
import com.db.DBOperations;
import com.db.DBStatement;
import com.db.DBUtils;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.json.JSONObject;

/**
 *
 * @author Praveen
 */
public class Main {

    private static String readFileContent(String filePath) throws IOException {
        String content = "";
        content = new String(Files.readAllBytes(Paths.get(filePath)));
        return content;
    }

    public static void main(String[] args) throws Exception {
        String configFile = null;
        if (args.length > 0) {
            configFile = args[0];
        } else {
            configFile = "D:\\exceltodb.json";
        }
        JSONObject config = new JSONObject(readFileContent(configFile));
        String excelFile = config.getString("excelFile");
        String workSheet = config.getString("workSheet");
        String url = config.getString("destDBURL");
        String username = config.getString("DBUserName");
        String password = config.getString("DBPassword");
        Map<String, String> mappings = new HashMap<>();
        JSONObject mappJson = config.getJSONObject("mappings");
        mappJson.keySet().forEach((key) -> {
            mappings.put(key, mappJson.getString(key));
        });
        String destTable = config.getString("destTable");
        System.out.println("Config is read");
        List<Record> records = readRecordsFromExcel(excelFile, workSheet, mappings);
        System.out.println("Records are read from excel sheet");
        importToDB(url,username,password,records, destTable);
        System.out.println("Import is Done.Check Your table in DB");
    }

    private static void importToDB(String url,String username,String password,List<Record> records, String destTable) throws SQLException, Exception {
        System.out.println("Establishing DB Connection");
        Connection con = DBConnection.getConnection(url,username,password);
        System.out.println("Connection succcess.Please be patient it may take more time based on no.of records");
        List<DBStatement> dbstmts = new ArrayList<>();
        for (Record record : records) {
            DBDMLStatement stmt = new DBDMLStatement(DBUtils.generateInsertStatement(destTable, record.getColumns().keySet()), new ArrayList(record.getColumns().values()));
            dbstmts.add(stmt);
        }
        DBOperations db = new DBOperations(con);
        db.executeTransaction(dbstmts);
    }

    public static List<Record> readRecordsFromExcel(String fileName, String sheetName, Map<String, String> mappings) throws FileNotFoundException, IOException, Exception {
        InputStream inp = new FileInputStream(fileName);
        Workbook wb = WorkbookFactory.create(inp);
        Sheet sheet = wb.getSheet(sheetName);
        Iterator<Row> rowIterator = sheet.rowIterator();
        Map<Integer, String> newMapping = new HashMap<Integer, String>();
        if (rowIterator.hasNext()) {
            Row header = rowIterator.next();
            Iterator<Cell> cellIterator = header.cellIterator();
            while (cellIterator.hasNext()) {
                Cell column = cellIterator.next();
                String headerName = column.getStringCellValue();
                if (mappings.get(headerName) != null) {
                    newMapping.put(column.getColumnIndex(), mappings.get(headerName));
                }
            }
        }
        Set<Integer> columnIndexes = newMapping.keySet();
        List<Record> records = new ArrayList<Record>();
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            Record xlRecord = new Record();
            for (int index : columnIndexes) {
                xlRecord.addValue(newMapping.get(index), getValue(row.getCell(index)));
            }
            if (isRowEmpty(row)) {
                break;
            }
            records.add(xlRecord);
        }
        wb.close();
        inp.close();
        return records;
    }

    public static Object getValue(Cell cell) throws Exception {
        Object value = null;
        CellType cellType = cell.getCellType();
        if (null == cellType) {
            throw new Exception("Unknown Cell Type:" + cellType);
        } else {
            switch (cellType) {
                case NUMERIC:
                    value = cell.getNumericCellValue();
                    break;
                case STRING:
                    value = cell.getStringCellValue();
                    break;
                case BLANK:
                    value = "";
                    break;
                default:
                    throw new Exception("Unknown Cell Type:" + cellType);
            }
        }
        return value;
    }

    public static boolean isRowEmpty(Row row) {
        for (int c = row.getFirstCellNum(); c < row.getLastCellNum(); c++) {
            Cell cell = row.getCell(c);
            if (cell != null && cell.getCellType() != CellType.BLANK) {
                return false;
            }
        }
        return true;
    }

}
