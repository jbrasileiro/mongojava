package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.text.MessageFormat;
import java.util.Map;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

    private final MongoCollection<User> usersCollection;
    //TODO> Ticket: User Management - do the necessary changes so that the sessions collection
    //returns a Session object
    private final MongoCollection<Document> sessionsCollection;

    private final Logger log;

    @Autowired
    public UserDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        CodecRegistry pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        usersCollection = db.getCollection("users", User.class).withCodecRegistry(pojoCodecRegistry);
        log = LoggerFactory.getLogger(this.getClass());
        //TODO> Ticket: User Management - implement the necessary changes so that the sessions
        // collection returns a Session objects instead of Document objects.
        sessionsCollection = db.getCollection("sessions");
    }

    /**
     * Inserts the `user` object in the `users` collection.
     *
     * @param user - User object to be added
     * @return True if successful, throw IncorrectDaoOperation otherwise
     */
    public boolean addUser(User user) {
        //TODO > Ticket: Durable Writes -  you might want to use a more durable write concern here!

        if(this.getUser(user.getEmail()) != null) {
            return false;
        } else {
            usersCollection.withWriteConcern(WriteConcern.MAJORITY).insertOne(user); //
            return true;
        }

        //TODO > Ticket: Handling Errors - make sure to only add new users
        // and not users that already exist.
    }

    /**
     * Creates session using userId and jwt token.
     *
     * @param userId - user string identifier
     * @param jwt    - jwt string token
     * @return true if successful
     */
    public boolean createUserSession(String userId, String jwt) {
        //TODO> Ticket: User Management - implement the method that allows session information to be
        // stored in it's designated collection.

//        // Let's go ahead and instantiate our document
//        Document doc1 = new Document("title", "Final Fantasy");
//        doc1.put("year", 2003);
//        doc1.put("label", "Square Enix");
//
//        // and instead of going to the database, run a query to check if the
//        // document already exists, we are going to emit an update command
//        // with the flag $upsert: true.
//
//        // We set a query predicate that finds the video game based on his title
//        Bson query = new Document("title", "Final Fantasy");
//
//        // And we try to updated. If we do not provide the upsert flag
//        UpdateResult resultNoUpsert = videoGames.updateOne(query, new Document("$set", doc1));
//
//        // if the document does not exist, the number of matched documents
//        Assert.assertEquals(0, resultNoUpsert.getMatchedCount());
//        // should be 0, so as the number of modified documents.
//        Assert.assertNotEquals(1, resultNoUpsert.getModifiedCount());
//
//        // on the other hand, if we do provide an upsert flag by setting the
//        // UpdateOptions document
//        UpdateOptions options = new UpdateOptions();
//        options.upsert(true);
//
//        // and adding those options to the update method.
//        UpdateResult resultWithUpsert =
//                videoGames.updateOne(query, new Document("$set", doc1), options);

        Document doc = new Document("user_id", userId);
        doc.append("jwt", jwt);

        UpdateOptions options = new UpdateOptions();
        options.upsert(true);

        Bson query = new Document("user_id", userId);
        UpdateResult resultWithUpsert =
                sessionsCollection.updateOne(query, new Document("$set", doc), options);


//        sessionsCollection.insertOne(doc);

        return true;
        //TODO > Ticket: Handling Errors - implement a safeguard against
        // creating a session with the same jwt token.
    }

    /**
     * Returns the User object matching the an email string value.
     *
     * @param email - email string to be matched.
     * @return User object or null.
     */
    public User getUser(String email) {
        User user = null;
        //TODO> Ticket: User Management - implement the query that returns the first User object.
        user = usersCollection.find(new Document("email", email)).limit(1).iterator().tryNext();
        return user;
    }

    /**
     * Given the userId, returns a Session object.
     *
     * @param userId - user string identifier.
     * @return Session object or null.
     */
    public Session getUserSession(String userId) {
        //TODO> Ticket: User Management - implement the method that returns Sessions for a given
        // userId

        Document sessionDoc = sessionsCollection.find(new Document("user_id", userId)).limit(1).iterator().tryNext();

        Session session = new Session();
        if (sessionDoc != null) {
            session.setUserId(sessionDoc.getString("user_id"));
            session.setJwt(sessionDoc.getString("jwt"));
            return session;
        } else {
            return null;
        }
    }

    public boolean deleteUserSessions(String userId) {
        //TODO> Ticket: User Management - implement the delete user sessions method
        DeleteResult delResult = sessionsCollection.deleteOne(new Document("user_id", userId));

        return (delResult.getDeletedCount() == 1);
    }

    /**
     * Removes the user document that match the provided email.
     *
     * @param email - of the user to be deleted.
     * @return true if user successfully removed
     */
    public boolean deleteUser(String email) {
        // remove user sessions
        //TODO> Ticket: User Management - implement the delete user method
        //TODO > Ticket: Handling Errors - make this method more robust by
        // handling potential exceptions.
        this.deleteUserSessions(email);
        DeleteResult delResult = usersCollection.deleteOne(new Document("email", email));

        return (delResult.getDeletedCount() == 1);
    }

    /**
     * Updates the preferences of an user identified by `email` parameter.
     *
     * @param email           - user to be updated email
     * @param userPreferences - set of preferences that should be stored and replace the existing
     *                        ones. Cannot be set to null value
     * @return User object that just been updated.
     */
    public boolean updateUserPreferences(String email, Map<String, ?> userPreferences) {
        //TODO> Ticket: User Preferences - implement the method that allows for user preferences to
        // be updated.

        if (userPreferences == null) return false;

        Document prefs = new Document("preferences", userPreferences);

        Bson query = new Document("email", email);
        UpdateResult result = usersCollection.updateOne(query, new Document("$set", prefs));


        //TODO > Ticket: Handling Errors - make this method more robust by
        // handling potential exceptions when updating an entry.
        return (result.getModifiedCount() == 1);
    }
}
