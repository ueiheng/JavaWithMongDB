import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.inc;

public class QuickTourTest {

    private MongoClient mongoClient;

    private MongoDatabase database;

    /**
     * Connect to a Single MongoDB instance
     */
    @Before
    public void before() {
        mongoClient = new MongoClient("127.0.0.1", 27017);
        database = mongoClient.getDatabase("test-xh");

    }

    @After
    public void after() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    /**
     * To create the document using the Java driver, use the Document class.
     * For example, consider the following JSON document:
     * {
     * "name" : "MongoDB",
     * "type" : "database",
     * "count" : 1,
     * "versions": [ "v3.2", "v3.0", "v2.6" ],
     * "info" : { x : 203, y : 102 }
     * }
     */
    @Test
    public void insertSingleDocument() {
        Document doc = new Document("name", "MongoDB")
                .append("type", "database")
                .append("count", 1)
                .append("versions", Arrays.asList("v3.2", "v3.0", "v2.6"))
                .append("info", new Document("x", 203).append("y", 102));
        MongoCollection<Document> collection = database.getCollection("customer");
        collection.insertOne(doc);
    }

    /**
     * Insert Multiple Documents
     * To add multiple documents, you can use the collection’s insertMany() method which takes a list of documents to insert.
     */
    @Test
    public void insertMutipleDocuments() {
        List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < 100; i++) {
            documents.add(new Document("i", i));
        }
        MongoCollection<Document> collection = database.getCollection("customer");
        collection.insertMany(documents);
    }

    /**
     * To return the first document in the collection, use the find() method without any parameters and chain to find() method the first() method.
     * If the collection is empty, the operation returns null.
     */
    @Test
    public void countDocumentsInACollection() {
        MongoCollection<Document> collection = database.getCollection("customer");
        System.out.println(collection.count());
    }

    /**
     * To query the collection, you can use the collection’s find() method.
     * You can call the method without any arguments to query all documents in a collection or pass a filter to query for documents that match the filter criteria.
     * The find() method returns a FindIterable() instance that provides a fluent interface for chaining other methods.
     */
    @Test
    public void queryTheCollection() {
        /**
         * Find the First Document in a Collection
         */
        MongoCollection<Document> collection = database.getCollection("customer");
        Document myDoc = collection.find().first();
        System.out.println(myDoc.toJson());

        /**
         * Find All Documents in a Collection
         */
        /**
         * The following example retrieves all documents in the collection and prints the returned documents (101 documents):
         */
        MongoCursor<Document> cursor = collection.find().iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
        /**
         * Although the following idiom for iteration is permissible, avoid its use as the application can leak a cursor if the loop terminates early:
         */
        for (Document cur : collection.find()) {
            System.out.println(cur.toJson());
        }
    }

    /**
     * To query for documents that match certain conditions, pass a filter object to the find() method.
     * To facilitate creating filter objects, Java driver provides the Filters helper.
     */
    @Test
    public void specifyAQueryFilter() {
        /**
         * Get A Single Document That Matches a Filter
         * For example, to find the first document where the field i has the value 71, pass an eq filter object to specify the equality condition:
         */
        MongoCollection<Document> collection = database.getCollection("customer");
        Document myDoc = collection.find(eq("i", 71)).first();
        System.out.println(myDoc.toJson());

        System.out.println("----------------------------");
        /**
         * Get All Documents That Match a Filter
         * The following example returns and prints all documents where "i" > 50:
         */
        Block<Document> printBlock = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        };
        collection.find(gt("i", 50)).forEach(printBlock);
        System.out.println("----------------------------");
        /**
         * The example uses the forEach method on the FindIterable object to apply a block to each document.
         * To specify a filter for a range, such as 50 < i <= 100, you can use the and helper:
         */
        collection.find(and(gt("i", 50), lte("i", 100))).forEach(printBlock);

    }

    /**
     * To update documents in a collection, you can use the collection’s updateOne and updateMany methods.
     * Pass to the methods:
     * A filter object to determine the document or documents to update. To facilitate creating filter objects, Java driver provides the Filters helper. To specify an empty filter (i.e. match all documents in a collection), use an empty Document object.
     * An update document that specifies the modifications. For a list of the available operators, see update operators.
     * The update methods return an UpdateResult which provides information about the operation including the number of documents modified by the update.
     */
    @Test
    public void updateDocuments() {
        // Update a Single Document
        MongoCollection<Document> collection = database.getCollection("customer");
        collection.updateOne(eq("i", 10), new Document("$set", new Document("i", 110)));
        // Update Multiple Documents
        UpdateResult updateResult = collection.updateMany(lt("i", 100), inc("i", 100));
        System.out.println(updateResult.getModifiedCount());
    }

    /**
     * To delete documents from a collection, you can use the collection’s deleteOne and deleteMany methods.
     * Pass to the methods a filter object to determine the document or documents to delete. To facilitate creating filter objects, Java driver provides the Filters helper. To specify an empty filter (i.e. match all documents in a collection), use an empty Document object.
     * The delete methods return a DeleteResult which provides information about the operation including the number of documents deleted.
     */
    @Test
    public void deleteDocuments() {
        /**
         * To delete at most a single document that match the filter, use the deleteOne method:
         * The following example deletes at most one document that meets the filter i equals 110:
         */
        MongoCollection<Document> collection = database.getCollection("customer");
        collection.deleteOne(eq("i", 110));
        /**
         * Delete All Documents That Match a Filter
         * To delete all documents matching the filter use the deleteMany method.
         * The following example deletes all documents where i is greater or equal to 100:
         */
        DeleteResult deleteResult = collection.deleteMany(gte("i", 100));
        System.out.println(deleteResult.getDeletedCount());
    }

    /**
     * To create an index on a field or fields, pass an index specification document to the createIndex() method.
     * An index key specification document contains the fields to index and the index type for each field:
     */
    @Test
    public void createIndexes() {
//        new Document(<field1>, <type1>).append(<field2>, <type2>) ...
//        For an ascending index type, specify 1 for <type>.
//        For a descending index type, specify -1 for <type>.
        MongoCollection<Document> collection = database.getCollection("customer");
        collection.createIndex(new Document("i", 1));
    }


}