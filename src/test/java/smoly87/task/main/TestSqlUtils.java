/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package smoly87.task.main;

import java.math.BigInteger;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import static smoly87.task.main.StringUtils.LINE_SEPARATOR;

/**
 *
 * @author Andrey
 */
public class TestSqlUtils {
    protected static EntityManager em;

    public static EntityManager getEm() {
        return em;
    }

    public static void setEm(EntityManager em) {
        TestSqlUtils.em = em;
    }
    
    public static int getDifferRowsCount(String mainTable, String subordinateTable){
        String query = String.join(LINE_SEPARATOR,
          "SELECT a.ID FROM @mainTable as a LEFT  JOIN @subordinateTable as b ON a.id = b.id Where b.id IS null UNION",
          "SELECT a.ID FROM @subordinateTable as a LEFT  JOIN @mainTable as b ON a.id = b.id Where b.id IS null UNION",
          "SELECT a.id FROM @mainTable as a JOIN @subordinateTable as b ON a.id = b.id WHERE a.Name != b.Name OR a.Description != b.Description");
        query = query.replaceAll("@mainTable", mainTable)
                     .replaceAll("@subordinateTable", subordinateTable);
        
        return em.createNativeQuery(query).getResultList().size();
    }
    
     public static  Query createQuery(String table, String query){
       query = query.replace("@table", table) ;
       return em.createNativeQuery(query);
    }
    
   
     public static  void executeQuery(String table, String query, int id){
        createQuery(table, query)
                .setParameter("id", id)
                .executeUpdate();
    
    }
    
    
    public static void executeQuery(String table, String query){
        createQuery(table, query)
                .executeUpdate();
    
    }
    
    public static  int getTableSize(String tableName){
       String query = String.format("SELECT COUNT(id) FROM %s", tableName);
       BigInteger res = (BigInteger) em.createNativeQuery(query).getSingleResult();
       return res.intValue();
    }
    
    public static String getInsertQuery(String table,int id, String Name, String Description){
        return String.format("INSERT into %s(Id, Name, Description) VALUES(%d, '%s', '%s') ", table, id, Name, Description);
    }
}
