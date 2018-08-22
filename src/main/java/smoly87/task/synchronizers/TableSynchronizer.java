package smoly87.task.synchronizers;

import java.util.Date;
import javax.persistence.EntityManager;
import javax.persistence.Query;

public abstract class TableSynchronizer {

    protected abstract String getQueryString();
    protected String mainTable;
    protected String subordinateTable;
    protected EntityManager em;
    protected Date revisionDate;
       
    public TableSynchronizer( String mainTable, String subordinateTable) {
        this.mainTable = mainTable;
        this.subordinateTable = subordinateTable;

    }
    
    protected String processQuery(String query, String mainTable, String subordinateTable){
        String res = query.replace("@mainTable", mainTable);
        res = res.replace("@subordinateTable", subordinateTable);
        return res;
    }
    
    public String getQuery(String mainTable, String subordinateTable){
        String queryStr = getQueryString();
        return processQuery(queryStr, mainTable, subordinateTable);
    }
    
    public void setParams(EntityManager em, Date revisionDate){
         this.revisionDate = revisionDate;
         this.em = em;
    }
    
    public void synchronizeTablesBidirectional(){
        synchronizeTables(mainTable, subordinateTable);
        synchronizeTables(subordinateTable, mainTable);
    }
        
    protected Query createQuerySetParams(String query){ 
        Query queryObj = em.createNativeQuery(query)
                           .setParameter("revision_date", revisionDate);
        return queryObj;
    }
     
    public void synchronizeTables(String mainTable, String subordinateTable){
        String query = getQuery(mainTable, subordinateTable);
        createQuerySetParams(query).executeUpdate();
    }
}