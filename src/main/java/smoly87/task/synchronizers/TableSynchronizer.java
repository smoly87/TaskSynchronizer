package smoly87.task.synchronizers;

import java.util.Date;
import javax.persistence.EntityManager;
import javax.persistence.Query;

public abstract class TableSynchronizer {
    protected boolean useIdsExclusionCondititon = false;
    protected abstract String getQueryString();
    protected String table1;
    protected String table2;
    protected  EntityManager em;
    protected Date revisionDate;

    public TableSynchronizer( String table1, String table2) {
        this.table1 = table1;
        this.table2 = table2;

    }
    
    protected String processQuery(String query, String mainTable, String subordinateTable){
        String res = query.replace("@t1", mainTable);
        res = res.replace("@t2", subordinateTable);
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
    
    public void synchronisation(){
        executeQuery(table1, table2);
        executeQuery(table2, table1);
    }
        
    protected Query createQuerySetParams(String query){ 
        Query queryObj = em.createNativeQuery(query)
                           .setParameter("revision_date", revisionDate);
        return queryObj;
    }
     
    public void executeQuery(String mainTable, String subordinateTable){
        String query = getQuery(mainTable, subordinateTable);
       
        createQuerySetParams(query).executeUpdate();
    }
}
