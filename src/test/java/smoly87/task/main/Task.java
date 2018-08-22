/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package smoly87.task.main;

import java.util.Date;

/**
 *
 * @author Andrey
 */
public class Task {
    protected String Name;
    protected String Description;

    public Task(int id, String Name, String Description ) {
        this.Name = Name;
        this.Description = Description;
        this.id = id;
    }

    public String getName() {
        return Name;
    }

    public String getDescription() {
        return Description;
    }

    public int getId() {
        return id;
    }

    public Date getLastModified() {
        return LastModified;
    }
    protected int id;
    protected Date LastModified;
}
