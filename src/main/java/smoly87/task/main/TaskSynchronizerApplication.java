package smoly87.task.main;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class TaskSynchronizerApplication {

	public static void main(String[] args) {
		SpringApplication.run(TaskSynchronizerApplication.class, args);
		/*TaskSynchoronizer sync = new TaskSynchoronizer();
		sync.testQuery();*/
	}
}
