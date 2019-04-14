package demo.test;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@SpringBootTest
@RunWith(SpringRunner.class)
public class ImportFile {

    @Autowired
    private BulkProcessor bulkProcessor;

    @Test
    public void ImportFileByBulk() throws IOException {
        long s = System.currentTimeMillis();
        String path = "D:\\corpus\\test2.txt";
        FileInputStream fis = new FileInputStream(path);
        // 防止路径乱码   如果utf-8 乱码  改GBK     eclipse里创建的txt  用UTF-8，在电脑上自己创建的txt  用GBK
        InputStreamReader isr = new InputStreamReader(fis);
        BufferedReader br = new BufferedReader(isr);
        String line = "";
        int i = 1;
        IndexRequest indexRequest = new IndexRequest("index_t4", "corpus");
        while ((line = br.readLine()) != null) {
            //文档内容
            //准备json数据
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("line_num", i++);
            jsonMap.put("content", line);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String format = dateFormat.format(new Date());
            jsonMap.put("create_time", format);

            //通过client进行http的请求
            bulkProcessor.add(indexRequest.source(jsonMap));
        }

        long e = System.currentTimeMillis();
        System.out.println((e-s)/1000);

        br.close();
        isr.close();
        fis.close();
    }


}
