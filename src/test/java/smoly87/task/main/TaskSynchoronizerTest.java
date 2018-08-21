/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package smoly87.task.main;

import smoly87.task.main.TaskSynchoronizerService;
import com.fasterxml.classmate.AnnotationConfiguration;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import org.hibernate.SessionFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.auditing.config.AnnotationAuditingConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.Query;
import org.flywaydb.core.internal.util.jdbc.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

/**
 *
 * @author Andrey
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class TaskSynchoronizerTest {
    @Autowired
    TaskSynchoronizerService taskSync;

    @Autowired
    private EntityManager em;
        
    @Autowired
    PlatformTransactionManager platformTransactionManager;
    
    protected final int DELAY_MS = 30;
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
        fillTestRecords("Task_Definition");
        fillTestRecords("Task_Definition_Mirror");
    }

    @After
    public void tearDown() {
        
    }

    protected void delay() throws InterruptedException{
       Thread.sleep(DELAY_MS);
    }
    
    protected void testUpdateSync(String sourceOfChangesTable) throws InterruptedException{
        taskSync.setRevisionDateByRecords();
        delay();
        
        String query = "Update @table SET Name='Task_', Description='Description_'Where id = :id";
        executeQuery("Task_Definition_Mirror", query, 2);
        TransactionTemplate transactionTemplate = new TransactionTemplate(platformTransactionManager);
        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
           @Override
           protected void doInTransactionWithoutResult(TransactionStatus status) {
              em.createNativeQuery("Update Task_Definition SET Name='Task_', Description='Description_' Where id = 2").executeUpdate();
          }
        });
       
        taskSync.synchroniseData();
    }
    
    @Test
    @Transactional
    public void testUpdateSyncMainIsTaskDefinition() throws InterruptedException, SQLException{ 
        testUpdateSync("Task_Definition");
        assertEquals(0, getDifferRowsCount());
    }
    
    @Test
    @Transactional
    public void testUpdateSyncMainIsTaskDefinitionMirror() throws InterruptedException, SQLException{ 
        testUpdateSync("Task_Definition_Mirror");
        assertEquals(0, getDifferRowsCount());
    }
    
    @Test	
    @Transactional
    public void testInsertSync() throws InterruptedException{ 
        //Insert record to one table only
       testInsert("Task_Definition");
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
        taskSync.synchroniseData();
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
    protected void executeQueryWithSync(String table, String query){
        createQuery(table, query)
                .executeUpdate();;
        taskSync.synchroniseData();
    }
    
    protected int getDifferRowsCount(){
        
        String query = String.join("\n",
          "SELECT a.ID FROM task_definition as a LEFT  JOIN task_definition_MIRROR as b ON a.id = b.id Where b.id IS null UNION",
          "SELECT a.ID  FROM task_definition_MIRROR as a LEFT  JOIN task_definition as b ON a.id = b.id Where b.id IS null UNION",
          "SELECT a.id FROM task_definition as a JOIN task_definition_MIRROR as b ON a.id = b.id WHERE a.Name != b.Name OR a.Description != b.Description");
        return em.createNativeQuery(query).getResultList().size();
    }

    protected void clearTables(){
         String query = String.join(";",
              "TRUNCATE Table Task_Definition",
              "TRUNCATE Table Task_Definition_Mirror"
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
