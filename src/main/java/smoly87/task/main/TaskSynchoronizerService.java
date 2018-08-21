package smoly87.task.main;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.persistence.*;
import javax.transaction.Transactional;
import java.util.Date;
import java.util.LinkedList;


import smoly87.task.synchronizers.DeleteSynchronizer;
import smoly87.task.synchronizers.InsertSynchronizer;
import smoly87.task.synchronizers.TableSynchronizer;
import smoly87.task.synchronizers.UpdateSynchronizer;

@Service
public class TaskSynchoronizerService {

    protected LinkedList<TableSynchronizer> tableSynchronisers;
    protected String table1 = "TASK_DEFINITION";
    protected String table2 = "TASK_DEFINITION_MIRROR";
    protected Date revisionDate;
    @Autowired
    private EntityManager em;
    
    public TaskSynchoronizerService() {
        tableSynchronisers = new LinkedList<>();
        tableSynchronisers.add(new UpdateSynchronizer(table1, table2));
        tableSynchronisers.add(new InsertSynchronizer(table1, table2));
        tableSynchronisers.add(new DeleteSynchronizer(table1, table2));
    }
    
    @Scheduled(fixedRate = 2000)
    @Transactional
    public void synchronizationDaemon(){
        synchroniseData();
    }
    
    protected void executeQuery(TableSynchronizer synchroniser, String mainTable, String subordinateTable){
        String query = synchroniser.getQuery(table1, table2);
        em.createNativeQuery(query)
          .setParameter("revision_date", revisionDate)
          .executeUpdate();
    }
    
    @Transactional
    public void synchroniseData(){
         synchroniseTables();
         setSynchronisationDate("TASK_DEFINITION");
         setSynchronisationDate("TASK_DEFINITION_MIRROR");
         revisionDate = (Date) em.createNativeQuery("SELECT CURRENT_TIMESTAMP").getSingleResult();
    }
    
    @Transactional
    public void setRevisionDateByRecords(){
        revisionDate = (Date) em.createNativeQuery("SELECT MAX(LAST_MODIFIED) FROM TASK_DEFINITION").getSingleResult();
    }
    
    public void setSynchronisationDate(String tableName){
        em.createNativeQuery(String.format("UPDATE %s SET LAST_MODIFIED=CURRENT_TIME()", tableName))
          .executeUpdate();
    }
    
    protected void synchroniseTables(){
        for(TableSynchronizer synchroniser:tableSynchronisers){
            synchroniser.setParams(em, revisionDate);
            synchroniser.synchronisation();
        }
    }

}
