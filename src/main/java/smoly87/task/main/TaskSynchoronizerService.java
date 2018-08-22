package smoly87.task.main;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.persistence.*;
import javax.transaction.Transactional;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;


import smoly87.task.synchronizers.DeleteSynchronizer;
import smoly87.task.synchronizers.InsertSynchronizer;
import smoly87.task.synchronizers.TableSynchronizer;
import smoly87.task.synchronizers.UpdateSynchronizer;

@Service
public class TaskSynchoronizerService {

    protected LinkedList<TableSynchronizer> tableSynchronisers;
    protected final String mainTable = "TASK_DEFINITION";
    protected final String subordinateTable = "TASK_DEFINITION_MIRROR";
    protected Date revisionDate;
    @Autowired
    private EntityManager em;
    
    public String getMainTable() {
        return mainTable;
    }

    public String getSubordinateTable() {
        return subordinateTable;
    }

    public Date getRevisionDate() {
        return revisionDate;
    }

    public void setRevisionDate(Date revisionDate) {
        this.revisionDate = revisionDate;
    }
 
    public TaskSynchoronizerService() {
        tableSynchronisers = new LinkedList<>();
        tableSynchronisers.add(new UpdateSynchronizer(mainTable, subordinateTable));
        tableSynchronisers.add(new InsertSynchronizer(mainTable, subordinateTable));
        tableSynchronisers.add(new DeleteSynchronizer(mainTable, subordinateTable));
    }
    
    @Scheduled(fixedRate = 1000)
    @Transactional
    public void synchronizationDaemon(){
        synchronizeTables();
    }
    
    protected void executeQuery(TableSynchronizer synchroniser, String mainTable, String subordinateTable){
        String query = synchroniser.getQuery(mainTable, subordinateTable);
        em.createNativeQuery(query)
          .setParameter("revision_date", revisionDate)
          .executeUpdate();
    }
    
    @Transactional
    public void synchronizeTables(){
         performSynchronizers();
         setSynchronisationDate(mainTable);
         setSynchronisationDate(subordinateTable);
         revisionDate = (Date) em.createNativeQuery("SELECT CURRENT_TIMESTAMP").getSingleResult();
    }
    
    @Transactional
    public Date getMaxDateByRecords(){
        String query = "SELECT MAX(last_modified) FROM task_definition  UNION SELECT MAX(last_modified) FROM task_definition_mirror";
        
        List <Date> itemsList = em.createNativeQuery(query).getResultList();
        Date maxDate = null;
        
        for(Date curValue : itemsList){     
            if(maxDate != null){
               if(curValue.compareTo(maxDate) > 0){
                  maxDate = curValue;
               }
            } else{
                maxDate = curValue;
            }
        }
        return maxDate;
    }
     
    public void setRevisionDateByRecords(){
       revisionDate = getMaxDateByRecords();
        
    }
    public void setSynchronisationDate(String tableName){
        em.createNativeQuery(String.format("UPDATE %s SET LAST_MODIFIED=CURRENT_TIME()", tableName))
          .executeUpdate();
    }
    
    protected void performSynchronizers(){
        for(TableSynchronizer synchroniser:tableSynchronisers){
            synchroniser.setParams(em, revisionDate);
            synchroniser.synchronizeTablesBidirectional();
        }
    }

}
