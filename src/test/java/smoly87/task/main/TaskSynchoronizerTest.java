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
import javax.persistence.Query;

import org.springframework.transaction.PlatformTransactionManager;

import static smoly87.task.main.SQLStringUtils.LINE_SEPARATOR;
/**
 *
 * @author Andrey
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class TaskSynchoronizerTest {    
    protected final int DELAY_MS = 100;
    protected final Integer ID_FOR_UPDATE = 2;
    
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
        taskSync.setRevisionDateByRecords();
        updateRecord(sourceOfChangesTable, ID_FOR_UPDATE, "Task_", "Description_" );    
        delay();
        taskSync.synchronizeTables();
        
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
    

    @Test
    @Transactional
    public void testUpdateSyncMainIsTaskDefinition() throws InterruptedException, SQLException{ 
        testUpdateSync(taskSync.getMainTable());
        assertEquals(0, getDifferRowsCount());
    }
    
    @Test
    @Transactional
    public void testUpdateSyncMainIsTaskDefinitionMirror() throws InterruptedException, SQLException{ 
        testUpdateSync(taskSync.getSubordinateTable());
        assertEquals(0, getDifferRowsCount());
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
        //Insert record to one table only
      
       
       testInsert(taskSync.getMainTable());
       
       int tableSizeAfter = getTableSize(taskSync.getMainTable());
       assertEquals(0, getDifferRowsCount());
    }
    
    @Test	
    @Transactional
    public void testInsertMirrorSync() throws InterruptedException{ 
        //Insert record to one table only
       testInsert("Task_Definition_Mirror");
       assertEquals(0, getDifferRowsCount());
    }
    
    protected void testInsert(String tableName) throws InterruptedException{
        delay();
        String query = getInsertQuery("@table", 3,  "Task3", "Description3");
        executeQueryWithSync(tableName, query);
    }
    
    @Test
    @Transactional
    public void testDeleteSync() throws InterruptedException{ 
        //Delete record from one table only
        testDelete("Task_Definition_Mirror");
        assertEquals(0, getDifferRowsCount());
    }
    
    @Test
    @Transactional
    public void testDeleteSyncMirror() throws InterruptedException{ 
        //Delete record from one table only
        testDelete("Task_Definition_Mirror");
        assertEquals(0, getDifferRowsCount());
    }
    
    protected void testDelete(String tableName) throws InterruptedException{
        taskSync.setRevisionDateByRecords();
        delay();
        String query = "DELETE FROM @table Where id = :id";
        executeQuery(tableName, query, 2);
        taskSync.synchronizeTables();
    }
   
    protected Query createQuery(String table, String query){
       query = query.replace("@table", table) ;
       return em.createNativeQuery(query);
    }
    
    @Transactional
    protected void executeQuery(String table, String query, int id){
        createQuery(table, query)
                .setParameter("id", id)
                .executeUpdate();
    }
    
    @Transactional
    protected void executeQuery(String table, String query){
        createQuery(table, query)
                .executeUpdate();
    
    }
    
    protected void executeQueryWithSync(String table, String query){
        executeQuery(table, query);
        taskSync.synchronizeTables();
    }
    
    protected int getTableSize(String tableName){
       String query = String.format("SELECT COUNT(id) FROM %s", tableName);
       BigInteger res = (BigInteger) em.createNativeQuery(query).getSingleResult();
       return res.intValue();
    }
    
    protected int getDifferRowsCount(){
        
        String query = String.join(LINE_SEPARATOR,
          "SELECT a.ID FROM task_definition as a LEFT  JOIN task_definition_MIRROR as b ON a.id = b.id Where b.id IS null UNION",
          "SELECT a.ID FROM task_definition_MIRROR as a LEFT  JOIN task_definition as b ON a.id = b.id Where b.id IS null UNION",
          "SELECT a.id FROM task_definition as a JOIN task_definition_MIRROR as b ON a.id = b.id WHERE a.Name != b.Name OR a.Description != b.Description");
        return em.createNativeQuery(query).getResultList().size();
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
    
    protected String getInsertQuery(String table,int id, String Name, String Description){
        return String.format("INSERT into %s(Id, Name, Description) VALUES(%d, '%s', '%s') ", table, id, Name, Description);
    }
}
