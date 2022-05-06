package es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * @Author 李文凯
 * 2022/5/6 20:43
 */
public class ESTestClient {
    public static void main(String[] args) throws IOException {
        // 创建ES客户端
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );
        // 创建索引
//        createIndex(esClient);

        // 查询索引
//        getIndex(esClient);

        // 删除索引
//        deleteIndex(esClient);

        // 插入数据
//        docInsert(esClient);

        // 局部更新
        docUpdate(esClient);

        // 关闭es客户端
        esClient.close();
    }

    /**
     * 创建索引
     *
     * @param esClient -
     */
    static void createIndex(RestHighLevelClient esClient) throws IOException {
        // 创建索引
        CreateIndexRequest user = new CreateIndexRequest("user");
        CreateIndexResponse createIndexResponse = esClient.indices().create(user, RequestOptions.DEFAULT);
        // 响应状态
        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println("索引操作" + acknowledged);
    }

    /**
     * 查询索引
     *
     * @param esClient -
     */
    static void getIndex(RestHighLevelClient esClient) throws IOException {
        // 查询索引
        GetIndexRequest user = new GetIndexRequest("user");
        GetIndexResponse getIndexResponse = esClient.indices().get(user, RequestOptions.DEFAULT);
        // 响应结果
        System.out.println(getIndexResponse.getAliases());
        System.out.println(getIndexResponse.getMappings());
        System.out.println(getIndexResponse.getSettings());
    }

    /**
     * 删除索引
     *
     * @param esClient -
     * @throws IOException -
     */
    static void deleteIndex(RestHighLevelClient esClient) throws IOException {
        DeleteIndexRequest user = new DeleteIndexRequest("user");
        AcknowledgedResponse delete = esClient.indices().delete(user, RequestOptions.DEFAULT);
        // 响应状态
        System.out.println("删除结果" + delete.isAcknowledged());
    }

    /**
     * 插入数据
     *
     * @param esClient -
     * @throws IOException -
     */
    static void docInsert(RestHighLevelClient esClient) throws IOException {
        IndexRequest request = new IndexRequest();
        request.index("user").id("1001");
        User user = new User();
        user.setName("zhangsan");
        user.setAge(30);
        user.setSex("男");

        // 向ES插入数据，必须将数据转换为JSON格式
        ObjectMapper mapper = new ObjectMapper();
        String userJson = mapper.writeValueAsString(user);
        request.source(userJson, XContentType.JSON);

        // 响应结果
        IndexResponse response = esClient.index(request, RequestOptions.DEFAULT);
        System.out.println(response.getResult());
    }

    /**
     * 局部更新
     *
     * @param esClient -
     * @throws IOException -
     */
    static void docUpdate(RestHighLevelClient esClient) throws IOException {
        //修改数据
        UpdateRequest request = new UpdateRequest();
        request.index("user").id("1001");
        request.doc(XContentType.JSON, "sex", "女");
        UpdateResponse response = esClient.update(request, RequestOptions.DEFAULT);
        System.out.println(response.getResult());

    }
}
