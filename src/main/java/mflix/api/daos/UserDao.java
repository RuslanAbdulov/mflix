package mflix.api.daos;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneOptions;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.setOnInsert;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

  private final MongoCollection<User> usersCollection;
  //> Ticket: User Management - do the necessary changes so that the sessions collection
  //returns a Session object
  private final MongoCollection<Session> sessionsCollection;

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
    //> Ticket: User Management - implement the necessary changes so that the sessions
    // collection returns a Session objects instead of Document objects.
    sessionsCollection = db.getCollection("sessions", Session.class).withCodecRegistry(pojoCodecRegistry);
  }

  /**
   * Inserts the `user` object in the `users` collection.
   *
   * @param user - User object to be added
   * @return True if successful, throw IncorrectDaoOperation otherwise
   */
  public boolean addUser(User user) {
    //> Ticket: Durable Writes -  you might want to use a more durable write concern here!
      //WriteConcern
      //> Ticket: Handling Errors - make sure to only add new users
      // and not users that already exist.
      try {
          usersCollection.withWriteConcern(WriteConcern.MAJORITY).insertOne(user);
      } catch (MongoWriteException e) {
          if(e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
              throw new IncorrectDaoOperation("duplicated key");
          } else {
              return false;
          }
      }
        return true;
  }

  /**
   * Creates session using userId and jwt token.
   *
   * @param userId - user string identifier
   * @param jwt - jwt string token
   * @return true if successful
   */
  public boolean createUserSession(String userId, String jwt) {
      //> Ticket: User Management - implement the method that allows session information to be
      // stored in it's designated collection.
      Bson filter = eq("user_id", userId);
      Bson update = set("jwt", jwt);

      UpdateOptions options = new UpdateOptions();
      options.upsert(true);
      //> Ticket: Handling Errors - implement a safeguard against
      // creating a session with the same jwt token.
      try {
          sessionsCollection.updateOne(filter, update, options);
      } catch (MongoException e) {
          return false;
      }
      return true;
  }

  /**
   * Returns the User object matching the an email string value.
   *
   * @param email - email string to be matched.
   * @return User object or null.
   */
  public User getUser(String email) {
    User user;
    //> Ticket: User Management - implement the query that returns the first User object.
      Bson filter = eq("email", email);
      user = usersCollection.find(filter).iterator().tryNext();
    return user;
  }

  /**
   * Given the userId, returns a Session object.
   *
   * @param userId - user string identifier.
   * @return Session object or null.
   */
  public Session getUserSession(String userId) {
    //> Ticket: User Management - implement the method that returns Sessions for a given
    // userId
      Bson filter = eq("user_id", userId);
      Session session = sessionsCollection.find(filter).iterator().tryNext();
    return session;
  }

  public boolean deleteUserSessions(String userId) {
    //> Ticket: User Management - implement the delete user sessions method
      Bson filter = eq("user_id", userId);
      DeleteResult deleteResult = sessionsCollection.deleteMany(filter);
    return deleteResult.wasAcknowledged();
  }

  /**
   * Removes the user document that match the provided email.
   *
   * @param email - of the user to be deleted.
   * @return true if user successfully removed
   */
  public boolean deleteUser(String email) {
    // remove user sessions
    //> Ticket: User Management - implement the delete user method
    //> Ticket: Handling Errors - make this method more robust by
    // handling potential exceptions.
      Bson filter = eq("email", email);
      try {
          usersCollection.deleteOne(filter);
          deleteUserSessions(email);
      } catch (MongoException e) {
          return false;
      }
      return true;
  }

  /**
   * Updates the preferences of an user identified by `email` parameter.
   *
   * @param email - user to be updated email
   * @param userPreferences - set of preferences that should be stored and replace the existing
   *     ones. Cannot be set to null value
   * @return User object that just been updated.
   */
  public boolean updateUserPreferences(String email, Map<String, ?> userPreferences) {
    //> Ticket: User Preferences - implement the method that allows for user preferences to
    // be updated.
      if (userPreferences == null) {
          throw new IncorrectDaoOperation("No-no-no");
      }

      Bson queryFilter = eq("email", email);
      //> Ticket: Handling Errors - make this method more robust by
      // handling potential exceptions when updating an entry.
      try {
          usersCollection.updateOne(queryFilter, set("preferences", userPreferences));
      } catch (MongoException e) {
          return false;
      }
      return true;
  }
}
