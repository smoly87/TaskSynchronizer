/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package smoly87.task.synchronizers;
/**
 *
 * @author Andrey
 */
public class InsertSynchronizer extends TableSynchronizer{

    public InsertSynchronizer(String table1, String table2) {
        super(table1, table2);
    }

    
    @Override
    protected String getQueryString() {
         String s = String.join("\n",
                 "INSERT INTO @t2(Id,  Name, Description)",
                 "(SELECT  a.Id, a.Name, a.Description FROM  @t1 as a Left JOIN @t2 as b ON a.id = b.id Where b.id IS null and a.LAST_MODIFIED > :revision_date)"

         );
         return s;
    }
    
}
