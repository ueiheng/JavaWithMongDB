import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import pojo.Address;
import pojo.Person;

import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class QuickTourPOJOsTest {

    private MongoClient mongoClient;

    private MongoDatabase database;

    /**
     * Connect to a Single MongoDB instance
     */
    @Before
    public void before() {

        CodecRegistry pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(), fromProviders(PojoCodecProvider.builder().automatic(true).build()));

//连接mongodb数据库
        mongoClient = new MongoClient("127.0.0.1", 27017);
        database = mongoClient.getDatabase("Restaurant_ordering_system").withCodecRegistry(pojoCodecRegistry);


    }
//断开数据库方法after
    @After
    public void after() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    /**
     * Inserting a POJO into MongoDB
     */
    @Test
    //添加一条person数据
    public void insertAPerson() {
        MongoCollection<Person> collection = database.getCollection("people", Person.class);
        Person man = new Person("wps", 21, new Address("St James Square", "London", "W1"));
        collection.insertOne(man);
    }

    /**
     * Inserting a POJO into MongoDB
     */
    @Test
    //添加多条person数据
    public void insertManyPersons() {
        MongoCollection<Person> collection = database.getCollection("people", Person.class);
        List<Person> people = asList(
                new Person("Charles Babbage", 45, new Address("5 Devonshire Street", "London", "W11")),
                new Person("Alan Turing", 28, new Address("Bletchley Hall", "Bletchley Park", "MK12")),
                new Person("Timothy Berners-Lee", 61, new Address("Colehill", "Wimborne", null))
        );
        collection.insertMany(people);
    }

    @Test
//    查询数据
    public void queryTheCollection() {
        MongoCollection<Person> collection = database.getCollection("people", Person.class);
        Block<Person> printBlock = new Block<Person>() {
            @Override
            public void apply(final Person person) {
                System.out.println(person);
            }
        };
        collection.find().forEach(printBlock);
    }

    /**
     * Specify a Query Filter
     */
    @Test
    public void queryFilter() {
        MongoCollection<Person> collection = database.getCollection("people", Person.class);
//        eq方法用于创建查询过滤器的静态方法。
        Person person = collection.find(eq("address.city", "Wimborne")).first();
        System.out.println(person);
        System.out.println("---------------------");
        /**
         * Get All Person Instances That Match a Filter
         The following example returns and prints everyone where "age" > 30:
         */
        Block<Person> printBlock = new Block<Person>() {
            @Override
            public void apply(final Person person) {
                System.out.println(person);
            }
        };
        collection.find(gt("age", 30)).forEach(printBlock);

    }

    /**
     * To update documents in a collection, you can use the collection’s updateOne and updateMany methods.
     * Pass to the methods:
     * A filter object to determine the document or documents to update. To facilitate creating filter objects, Java driver provides the Filters helper. To specify an empty filter (i.e. match all Persons in a collection), use an empty Document object.
     * An update document that specifies the modifications. For a list of the available operators, see update operators.
     * The update methods return an UpdateResult which provides information about the operation including the number of documents modified by the update
     */
    @Test
    public void updateDocuments() {
        /**
         * Update a Single Person
         * To update at most a single Person, use the updateOne method.
         */
        MongoCollection<Person> collection = database.getCollection("people", Person.class);
        collection.updateOne(eq("name", "虚幻"), combine(set("age", 23), set("name", "虚幻改")));
        /**
         * Update Multiple Persons
         */
        UpdateResult updateResult = collection.updateMany(not(eq("zip", null)), set("zip", null));
        System.out.println(updateResult.getModifiedCount());

    }

    @Test
    public void replaceAsinglePerson(){
        MongoCollection<Person> collection = database.getCollection("people", Person.class);
        Person ada = new Person("Ada Byron", 20, new Address("St James Square", "London", "W1"));
        collection.replaceOne(eq("name", "虚幻"), ada);

    }

    @Test
    public void deleteDocuments(){
        MongoCollection<Person> collection = database.getCollection("people", Person.class);
        /**
         * Delete a Single Person That Matches a Filter
         */
        collection.deleteOne(eq("address.city", "Wimborne"));
        /**
         * Delete All Persons That Match a Filter
         */
        DeleteResult deleteResult = collection.deleteMany(eq("address.city", "London"));
        System.out.println(deleteResult.getDeletedCount());

    }

}
