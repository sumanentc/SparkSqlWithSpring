package ai.cuddle.spark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;

/**
 * Created by suman.das on 11/30/17.
 */
@SpringBootApplication
@ComponentScan("ai.cuddle.spark.*")
@PropertySource("classpath:application.properties")
public class Application {
    public static void main(String[] args){
        SpringApplication.run(Application.class,args);

    }
}
