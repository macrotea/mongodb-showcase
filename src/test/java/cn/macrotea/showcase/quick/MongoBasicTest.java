package cn.macrotea.showcase.quick;

import com.mongodb.*;
import org.junit.*;

import java.util.Date;
import java.util.List;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * @author macrotea@qq.com
 * @since 2014-8-4 下午9:01
 */
public class MongoBasicTest {

    //本地启动mongodb服务器后才能正常运行此单元测试
    //mongod.exe -f _mongodb.config -logappend -rest

    //业务集合persons的数据样例:
    //{ "_id":{
    // "$oid":"53e613283dc8648ef92df4cf" },
    // "age":22,
    // "books":["JS", "JAVA", "C#", "MONGODB"],
    // "c":45,
    // "country":"Korea",
    // "e":77,
    // "email":"macrotea@qq.com",
    // "m":63,
    // "name":"zhangsuying",
    // "size":4
    // }

    //线程安全
    static MongoClient mgo = null;

    public static final String LOCAL_DB = "showcase-mgr";
    public static final String LOCAL_COLL = "persons";

    @BeforeClass
    public static void start() throws Exception {
        mgo = new MongoClient("localhost", 27017);
    }

    @AfterClass
    public static void end() throws Exception {
        mgo.close();
        mgo = null;
    }

    @Before
    public void setUp() throws Exception {
        DBObject dbObj = new BasicDBObject();

        //若不存在则创建
        db().getCollection(LOCAL_COLL);
        // db().createCollection(LOCAL_COLL, dbObj);
    }

    @After
    public void tearDown() throws Exception {
        persons().drop();
    }

    public DB db() {
        //若无此数据库则自动创建
        DB db = mgo.getDB(LOCAL_DB);

        //安全验证
        //boolean authed = db.authenticate("root", "root");
        //assertEquals(true, authed);

        return db;
    }

    public DBCollection persons() {
        return db().getCollection(LOCAL_COLL);
    }

    @Test
    public void test_version() throws Exception {
        //C:\Users\Lenovo > mongo-- version
        //MongoDB shell version:
        //2.4.9
        // FIXME macrotea@qq.com 2014-08-10 22:12:39 why?
        // ANSWER 原来getVersion()获得的是这个驱动的版本而非你本地的mongodb版本
        assertEquals("2.12.3", mgo.getVersion());
    }

    @Test
    public void test_db_exists() throws Exception {
        assertThat(mgo.getDatabaseNames().contains(LOCAL_DB)).isTrue();
    }

    public void insertOneDoc(int i) {
        BasicDBObject dbObj = new BasicDBObject("name", i + "macrotea").append("email", i + "macrotea@qq.com");
        dbObj.put("age", 20 * i);
        dbObj.put("addTime", new Date());
        dbObj.put("location", new BasicDBObject("x", 10 * i).append("y", 5 * i));
        persons().insert(dbObj);
    }

    @Test
    public void test_coll_exists() throws Exception {
        //prepare
        insertOneDoc(1);

        DBCursor cursor = null;
        try {
            //then
            cursor = persons().find();

            //check
            assertEquals(1, persons().count());
            assertEquals(1, cursor.count());
        } finally {
            //关闭游标
            cursor.close();
            if(cursor!=null){
                //关闭游标
                cursor.close();
            }
            //关闭游标
            cursor.close();
        }

    }

    @Test
    public void test_coll_check() throws Exception {
        //prepare
        insertOneDoc(1);

        //then
        //findOne比游标方便
        DBObject result = persons().findOne();

        //check
        assertEquals("1macrotea", result.get("name"));
        assertEquals("1macrotea@qq.com", result.get("email"));
        assertEquals(20, result.get("age"));
    }

    @Test
    public void test_coll_find_name() throws Exception {
        //prepare
        insertOneDoc(1);

        BasicDBObject query = new BasicDBObject("name", "1macrotea");

        //then
        DBCursor cursor = persons().find(query);

        //check
        assertThat(cursor.hasNext()).isFalse();
        assertEquals("1macrotea", cursor.next().get("name"));
    }

    @Test
    public void test_coll_find_age() throws Exception {
        //prepare
        insertOneDoc(1);
        insertOneDoc(2);

        BasicDBObject query = new BasicDBObject("age", new BasicDBObject("$lte", 50))
                .append("age", new BasicDBObject("$gte", 10));

        //then
        DBCursor cursor = persons().find(query);

        //check
        DBObject one = cursor.next();
        DBObject two = cursor.next();
        boolean hasThree = cursor.hasNext();

        assertEquals("1macrotea", one.get("name"));
        assertEquals(20, one.get("age"));

        assertEquals("2macrotea", two.get("name"));
        assertEquals(40, two.get("age"));

        assertThat(hasThree).isFalse();
    }

    @Test
    public void test_coll_index() throws Exception {
        //prepare
        insertOneDoc(1);
        insertOneDoc(2);

        BasicDBObject nameIndex = new BasicDBObject("name", 1);

        //then
        persons().createIndex(nameIndex);

        //when
        List<DBObject> indexes = persons().getIndexInfo();

        //check
        assertThat(indexes).hasSize(2);
        assertThat(indexes.get(0).get("name")).isEqualTo("_id_");

        //升序故而 name_1 ,若降序则: name_-1
        assertThat(indexes.get(1).get("name")).isEqualTo("name_1");

        //then
        persons().dropIndex(nameIndex);

        //when
        List<DBObject> indexesAgain = persons().getIndexInfo();

        //check again
        assertThat(indexesAgain).hasSize(1);//1
        assertThat(indexesAgain.get(0).get("name")).isEqualTo("_id_");
    }

    @Test
    public void test_doc_delete() throws Exception {
        //prepare
        insertOneDoc(1);

        //then
        DBObject result = persons().findOne();

        //check
        assertThat(result.get("_id")).isNotNull();

        //then
        DBObject query = new BasicDBObject("name", "1macrotea");
        WriteResult writeResult = persons().remove(query);

        //check again
        assertThat(writeResult.getN()).isEqualTo(1);//影响行数

        //then again
        DBObject result2 = persons().findOne();

        //check 3th
        assertThat(result2).isNull();
    }

    @Test
    public void test_doc_update_simple() throws Exception {
        //prepare
        insertOneDoc(1);


        DBObject query = new BasicDBObject("name", "1macrotea");
        //update spec
        DBObject updated = persons().findOne();
        updated.put("name", "macrotea-handsome");

        //then
        WriteResult result = persons().update(query, updated, false, false);

        //check
        assertThat(result.getN()).isEqualTo(1);

        //when
        DBObject found = persons().findOne();
        assertThat(found.get("name")).isEqualTo("macrotea-handsome");
    }

    @Test
    public void test_doc_update_upsert() throws Exception {
        //prepare
        insertOneDoc(1);

        //不存在的查询条件,当upsert为true,则插入updated对象
        DBObject query = new BasicDBObject("name", "10000macrotea");

        //update spec
        DBObject updated = persons().findOne();
        updated.put("_id", "001");
        updated.put("name", "macrotea-handsome");

        //then
        // NOTICE macrotea@qq.com 2014-08-11 09:28:32 第三个参数为true
        WriteResult result = persons().update(query, updated, true, false);

        //check
        assertThat(result.getN()).isEqualTo(1);//影响行数
        assertThat(persons().find().count()).isEqualTo(2);

        //then
        DBCursor cursor = persons().find(new BasicDBObject("name", "macrotea-handsome"));

        //check again
        assertThat(cursor.hasNext()).isTrue();

        DBObject spec = cursor.next();
        assertThat(spec.get("name")).isEqualTo("macrotea-handsome");
        assertThat(spec.get("age")).isEqualTo(20);
    }

    @Test
    public void test_doc_update_fields_score() throws Exception {
        //prepare
        insertOneDoc(1);
        insertOneDoc(2);

        BasicDBObject query = new BasicDBObject("age", new BasicDBObject("$lte", 50)).append("age", new BasicDBObject("$gte", 10));

        //导致文档会多一个score字段
        DBObject updated = new BasicDBObject();
        updated.put("$set", new BasicDBObject("score",100));

        //then
        // NOTICE macrotea@qq.com 2014-08-11 09:28:32 第4个参数为true
        WriteResult result = persons().update(query, updated, false, true);

        //check
        assertThat(result.getN()).isEqualTo(2);//影响行数
        assertThat(persons().find().count()).isEqualTo(2);

        //then
        DBCursor cursor = persons().find(query);

        //check again

        //one
        assertThat(cursor.hasNext()).isTrue();
        DBObject one = cursor.next();
        assertThat(one.get("name")).isEqualTo("1macrotea");
        assertThat(one.get("score")).isEqualTo(100);
        //two
        assertThat(cursor.hasNext()).isTrue();
        DBObject two = cursor.next();
        assertThat(two.get("name")).isEqualTo("2macrotea");
        assertThat(two.get("score")).isEqualTo(100);
    }

    @Test
    public void test_doc_update_fields_age() throws Exception {
        //prepare
        insertOneDoc(1);

        //导致文档会多一个score字段
        BasicDBObject updated = new BasicDBObject();
        updated.put("$set", new BasicDBObject("age",100));

        //then
        WriteResult result = persons().update(new BasicDBObject(), updated, false, false);

        //check
        assertThat(result.getN()).isEqualTo(1);//影响行数
        assertThat(persons().find().count()).isEqualTo(1);

        //then
        DBObject found = persons().findOne();

        //check again
        assertThat(found).isNotNull();
        assertThat(found.get("name")).isEqualTo("1macrotea");
        assertThat(found.get("age")).isEqualTo(100);//不再是20
    }

    @Test
    public void test_doc_find_result_fields() throws Exception {
        //prepare
        insertOneDoc(1);
        insertOneDoc(2);

        BasicDBObject keys = new BasicDBObject();
        keys.put("name", true);

        //then
        DBCursor cursor = persons().find(null, keys);

        //check

        //one
        assertThat(cursor.hasNext()).isTrue();
        DBObject one = cursor.next();
        assertThat(one.get("name")).isEqualTo("1macrotea");
        assertThat(one.get("_id")).isNotNull();//id必须返回

        assertThat(one.get("age")).isNull();
        assertThat(one.get("addTime")).isNull();

        //two
        assertThat(cursor.hasNext()).isTrue();
        DBObject two = cursor.next();
        assertThat(two.get("name")).isEqualTo("2macrotea");
        assertThat(two.get("_id")).isNotNull();//id必须返回

        assertThat(two.get("age")).isNull();
        assertThat(two.get("addTime")).isNull();
    }

    @Test
    public void test_doc_find_page() throws Exception {
        //prepare
        insertOneDoc(1);
        insertOneDoc(2);
        insertOneDoc(3);
        insertOneDoc(4);//p2 start
        insertOneDoc(5);
        insertOneDoc(6);//p2 end
        insertOneDoc(7);
        insertOneDoc(8);

        BasicDBObject keys = new BasicDBObject();
        BasicDBObject query = new BasicDBObject();
        int page = 2;
        int pageSize = 3;

        //then
        DBCursor cursor = persons().find(query, keys).limit(pageSize).skip((page-1)*pageSize);

        //check

        //four
        assertThat(cursor.hasNext()).isTrue();
        DBObject four = cursor.next();
        assertThat(four.get("name")).isEqualTo("4macrotea");
        //five
        assertThat(cursor.hasNext()).isTrue();
        DBObject five = cursor.next();
        assertThat(five.get("name")).isEqualTo("5macrotea");
        //six
        assertThat(cursor.hasNext()).isTrue();
        DBObject six = cursor.next();
        assertThat(six.get("name")).isEqualTo("6macrotea");

    }

    @Test
    public void test_doc_find_page_sort() throws Exception {
        //prepare
        insertOneDoc(1);
        insertOneDoc(2);
        insertOneDoc(3);//p2 end
        insertOneDoc(4);
        insertOneDoc(5);//p2 start
        insertOneDoc(6);
        insertOneDoc(7);
        insertOneDoc(8);

        BasicDBObject keys = new BasicDBObject();
        BasicDBObject query = new BasicDBObject();
        BasicDBObject sort = new BasicDBObject("age",-1);
        int page = 2;
        int pageSize = 3;

        //then
        DBCursor cursor = persons().find(query, keys).limit(pageSize).skip((page-1)*pageSize).sort(sort);

        //check

        //five
        assertThat(cursor.hasNext()).isTrue();
        DBObject five = cursor.next();
        assertThat(five.get("name")).isEqualTo("5macrotea");
        //four
        assertThat(cursor.hasNext()).isTrue();
        DBObject four = cursor.next();
        assertThat(four.get("name")).isEqualTo("4macrotea");
        //three
        assertThat(cursor.hasNext()).isTrue();
        DBObject three = cursor.next();
        assertThat(three.get("name")).isEqualTo("3macrotea");

    }

}
