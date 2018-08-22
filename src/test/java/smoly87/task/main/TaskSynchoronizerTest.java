/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package smoly87.task.main;

import java.math.BigInteger;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.StringJoiner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import org.springframework.transaction.PlatformTransactionManager;

import static smoly87.task.main.TestSqlUtils.*;

/**
 *
 * @author Andrey
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class TaskSynchoronizerTest {    
    protected final int DELAY_MS = 100;
    protected final Integer ID_FOR_UPDATE = 2;
    protected final int ID_FOR_INSERT = 3;
    @Autowired
    TaskSynchoronizerService taskSync;

    @Autowired
    private EntityManager em;
        
    @Autowired
    PlatformTransactionManager platformTransactionManager;
    public TaskSynchoronizerTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before

    public void setUp() {
        TestSqlUtils.setEm(em);
        prepareTables();
    }
   
    protected void prepareTables(){
        clearTables();
        fillTestRecords(taskSync.getMainTable());
        fillTestRecords(taskSync.getSubordinateTable());
    }

    @After
    public void tearDown() {
        
    }

    protected void delay() throws InterruptedException{
       Thread.sleep(DELAY_MS);
    }

    protected void testUpdateSync(String sourceOfChangesTable) throws InterruptedException{
        TableSizeChecker tableSizeChecker = createTableSizeChecker();
        taskSync.setRevisionDateByRecords();
        updateRecord(sourceOfChangesTable, ID_FOR_UPDATE, "Task_", "Description_" );    
        delay();
        taskSync.synchronizeTables();
        assertEquals(true, isDifferenceEqualsTo(0, tableSizeChecker));
        assertEquals(0, getDifferRowsCount()); 
    } 
    
    @Transactional
    protected void updateRecord(String sourceOfChangesTable, int id, String Name, String Description){
       Date initDate = taskSync.getMaxDateByRecords();
       String query = "Update @table SET Name=:name, Description=:description, Last_Modified = DATEADD('MILLISECOND', +:delay, :init_date) Where id = :id";
         createQuery(sourceOfChangesTable, query)
                .setParameter("id", id)
                .setParameter("name", Name) 
                .setParameter("description", Description) 
                .setParameter("init_date", initDate)
                .setParameter("delay", DELAY_MS / 2)
                .executeUpdate();
    }
    
    @Transactional
    protected void insertRecord(String sourceOfChangesTable, int id, String Name, String Description){
       Date initDate = taskSync.getMaxDateByRecords();
       String query = "INSERT INTO @table(Id, Name, Description, Last_Modified) VALUES(:id, :name, :description, DATEADD('MILLISECOND', +:delay, :init_date))";
         createQuery(sourceOfChangesTable, query)
                .setParameter("id", id)
                .setParameter("name", Name) 
                .setParameter("description", Description) 
                .setParameter("init_date", initDate)
                .setParameter("delay", DELAY_MS / 2)
                .executeUpdate();
    }
    
    @Test
    @Transactional
    public void testUpdateSyncMainIsTaskDefinition() throws InterruptedException, SQLException{ 
        testUpdateSync(taskSync.getMainTable());
    }
    
    @Test
    @Transactional
    public void testUpdateSyncMainIsTaskDefinitionMirror() throws InterruptedException, SQLException{ 
        testUpdateSync(taskSync.getSubordinateTable());

    }
    
    /*This is Update case new test, which we disscused.
      At first we update main_table record, then mirror_table.
      As mirror_table Last_Modified will be greater, 
      so final value should be assigned to main table also.
      Eventually we should check that content of these tables is equal 
      and value of Name and Description in Main table should be assigned from mirror(Task__ and Description__)
    */
    @Test
    @Transactional
    public void testUpdateMainThenMirrorSync() throws InterruptedException, SQLException{ 
        final String NameFirstChange = "Task_";
        final String DescriptionFirstChange = "Description_";
        
        final String NameLastChange = "Task__";
        final String DescriptionLastChange = "Description__";
        
        taskSync.setRevisionDateByRecords();
        updateRecord(taskSync.getMainTable(), ID_FOR_UPDATE, NameFirstChange, DescriptionFirstChange);  
        updateRecord(taskSync.getSubordinateTable(), ID_FOR_UPDATE, NameLastChange, DescriptionLastChange);  
        delay();
        taskSync.synchronizeTables();
        
        Task mainTableTask = getRecordContent(taskSync.getMainTable(), ID_FOR_UPDATE);
        Task mirrorTableTask = getRecordContent(taskSync.getSubordinateTable(), ID_FOR_UPDATE);
             
        //Check that record content is equal to final changes(in both tables)
        assertEquals(NameLastChange, mainTableTask.getName());
        assertEquals(DescriptionLastChange, mainTableTask.getDescription());
        
        assertEquals(NameLastChange, mirrorTableTask.getName());
        assertEquals(DescriptionLastChange, mirrorTableTask.getDescription());
        
        //Check that there is no difference in both tables
        assertEquals(0, getDifferRowsCount());
    }
    
    protected Task getRecordContent(String tableName, int id){
         List<Object[]> itemsList =  em.createNativeQuery(String.format("SELECT  Id, Name, Description FROM %s Where id = %d", tableName, id) ).getResultList();
         Object[] values = itemsList.get(0);
         return new Task((int)values[0], (String)values[1], (String)values[2]);
    }
    
    @Test	
    @Transactional
    public void testInsertSync() throws InterruptedException{ 
       testInsert(taskSync.getMainTable());
    }
    
    @Test	
    @Transactional
    public void testInsertMirrorSync() throws InterruptedException{ 
       testInsert(taskSync.getSubordinateTable());

    }
    
    protected void testInsert(String tableName) throws InterruptedException{
        taskSync.setRevisionDateByRecords();
        TableSizeChecker tableSizeChecker = this.createTableSizeChecker();
        //Insert record to one table only
        insertRecord(tableName, ID_FOR_INSERT,  "Task3", "Description3");
        delay();
        taskSync.synchronizeTables();
        //Size of both table should be increased on 1
        assertEquals(true, isDifferenceEqualsTo(1, tableSizeChecker));
        assertEquals(0, getDifferRowsCount());
    }
    
    @Test
    @Transactional
    public void testDeleteSync() throws InterruptedException{ 
        testDelete(taskSync.getMainTable());
    }
    
    @Test
    @Transactional
    public void testDeleteSyncMirror() throws InterruptedException{ 
        testDelete(taskSync.getSubordinateTable());
    }
    
    protected void testDelete(String tableName) throws InterruptedException{
        TableSizeChecker tableSizeChecker = this.createTableSizeChecker();
        taskSync.setRevisionDateByRecords();
        delay();
        //Delete record from one table only
        String query = "DELETE FROM @table Where id = :id";
        executeQuery(tableName, query, 2);
        taskSync.synchronizeTables();
        //Table size of both tables should be decreased one 1, because one record is out
        assertEquals(true, isDifferenceEqualsTo(-1, tableSizeChecker));
        assertEquals(0, getDifferRowsCount());
    }
   
    protected int getDifferRowsCount(){
        return TestSqlUtils.getDifferRowsCount(taskSync.getMainTable(), taskSync.getSubordinateTable());
    }
    
    protected void executeQueryWithSync(String table, String query){
        executeQuery(table, query);
        taskSync.synchronizeTables();
    }
    
 
    protected TableSizeChecker createTableSizeChecker(){
        return new TableSizeChecker(getTableSize(taskSync.getMainTable()), 
                                    getTableSize(taskSync.getSubordinateTable()));
    }
    
    protected boolean isDifferenceEqualsTo(int delta,TableSizeChecker tableSizeChecker){
        return tableSizeChecker.isDifferenceEqualsAndEqualToDelta(
                             getTableSize(taskSync.getMainTable()), 
                             getTableSize(taskSync.getSubordinateTable()), 
                             delta
        );
    }
    
    protected void clearTables(){
         String query = String.join(";",
              String.format("TRUNCATE Table %s", taskSync.getMainTable()),
              String.format("TRUNCATE Table %s", taskSync.getSubordinateTable())
         );
         em.createNativeQuery(query).executeUpdate();
    }
   
    protected void fillTestRecords(String table){
         String query = String.join(";",
              getInsertQuery(table, 1, "Task1", "Description1"),
              getInsertQuery(table, 2, "Task2", "Description2")
         );
         em.createNativeQuery(query).executeUpdate();
    }
}
