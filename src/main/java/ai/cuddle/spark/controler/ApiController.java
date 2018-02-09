package ai.cuddle.spark.controler;

import ai.cuddle.spark.entity.Count;
import ai.cuddle.spark.entity.request.Analysis;
import ai.cuddle.spark.service.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Created by suman.das on 11/30/17.
 */
@RestController
@RequestMapping("api")
public class ApiController {

    @Autowired
    WordCount wordCount;

    @Autowired
    JDBCService jdbcService;

    @Autowired
    HiveService hiveService;

    @Autowired
    DataApiService dataApiService;

    @Autowired
    ParquetService parquetService;

    @RequestMapping("/wordcount")
    public ResponseEntity<List<Count>> words() {
        return new ResponseEntity<>(wordCount.count(), HttpStatus.OK);
    }

    @RequestMapping("/empcount")
    public ResponseEntity<List<Map<String,Object>>> empcount() {
        return new ResponseEntity<>(jdbcService.fetchEmployeeData(),HttpStatus.OK);
    }

    @RequestMapping("/maxSalary")
    public ResponseEntity<List<Map<String,Object>>> maxSalary() {
        return new ResponseEntity<>(jdbcService.fetchMaxSalaryEmployee(),HttpStatus.OK);
    }

    @RequestMapping("/sales/{brand}")
    public ResponseEntity<List<Map<String,Object>>> brandSales(@PathVariable("brand") String brand) {
        return new ResponseEntity<>(hiveService.brandSales(brand),HttpStatus.OK);
    }

    @RequestMapping("/sales")
    public ResponseEntity<List<Map<String,Object>>> sales() {
        return new ResponseEntity<>(hiveService.sales(),HttpStatus.OK);
    }

    @RequestMapping(value="/fetchData",method = RequestMethod.POST)
    public ResponseEntity<List<Map<String,Object>>> fetchData(@RequestBody Analysis analysis) {
        return new ResponseEntity<>(dataApiService.fetchData(analysis),HttpStatus.OK);
    }

    @RequestMapping("/save")
    public ResponseEntity saveData() throws Exception{
        //jdbcService.loadCSV();
        parquetService.loadData();
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @RequestMapping("/cache")
    public ResponseEntity cache() {
        hiveService.buildCache();
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @RequestMapping("/clear")
    public ResponseEntity clear() {
        hiveService.clearCache();
        return new ResponseEntity<>(HttpStatus.OK);
    }
}
