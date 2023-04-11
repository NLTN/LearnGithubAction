__all__ = ['UserBLL', 'DataSourceBLL', 'RawDataBLL', 'ScraperTaskBLL', 'HashtagBLL', 'TopicBLL', 'TwitterUserBLL', 'TweetBLL', 'Between']
from abc import ABC, abstractmethod
from dbconnection import DatabaseConnection
from DAL import *
from models import *
from dataclasses import dataclass
import re

@dataclass
class Between:
    low: int
    high: int
    inclusive:bool = True

class BaseBLL(ABC):
    def __init__(self, db:DatabaseConnection):
        self._db = db

    @abstractmethod
    def _get_entity(self) -> BaseDAL:
        pass

    def find_all(self):
        """ Retrieve all records from the table """
        session = self._db.create_session()
        try:
            EntityDAL = self._get_entity()
            repo = EntityDAL(session)
            results:list[any] = repo.find_all()
        except Exception as e:
            print(e)
            return None
        finally:
            session.close()
            return results
        
    def find_by_id(self, id):
        """ Find a single record by the given primary_key(id)

        Parameters:
            id (any): The primary_key(id)
        Returns:
            The recordset on success. Otherwise, returns None.
        """
        session = self._db.create_session()
        try:
            EntityDAL = self._get_entity()
            repo = EntityDAL(session)
            res = repo.find_by_id(id)
        except Exception as e:
            print(e)
            return None
        finally:
            session.close()
            return res

    def delete(self, id) -> int:
        """ Delete a row by id

        Parameters:
            id (any): The primary_key(id)
        Returns:
            int: The number of affected rows
        """
        session = self._db.create_session()
        try:
            EntityDAL = self._get_entity()
            repo = EntityDAL(session)
            res = repo.delete(id)
            session.commit()
        except Exception as e:
            print('***' * 50)
            print(e)
            session.rollback()
            return 0
        finally:
            session.close()
            return res
        
    def delete_all(self) -> int:
        """ Delete all records from the table

        Returns:
            int: The number of affected rows
        """
        session = self._db.create_session()
        try:
            EntityDAL = self._get_entity()
            repo = EntityDAL(session)
            result = repo.delete_all()
            session.commit()        
        except Exception as e:
            print(e)
            session.rollback()
        finally:
            session.close()
            return result

class UserBLL(BaseBLL):
    def _get_entity(self):
        return UserDAL
    
    def find_all(self) -> list[User] | None:
        """ Retrieve all records from the table """
        return super().find_all()
    
    def find_by_id(self, id) -> User | None:
        """ Find a single record by the given primary_key(id)

        Parameters:
            id (any): The primary_key(id)
        Returns:
            The recordset on success. Otherwise, returns None.
        """
        return super().find_by_id(id)
    
    def find_by_email(self, email):
        """ Find a single record by the given email

        Parameters:
            email (any): The user's email
        Returns:
            The recordset on success. Otherwise, returns None.
        """
        session = self._db.create_session()
        try:
            repo = UserDAL(session)
            result = repo.find_by_email(email)
        except Exception as e:
            print(e)
        finally:
            session.close()
            return result

    def insert(self, email:str, password:str):
        """
        Insert an item into the table

        Parameters:
            email (str): User's email
            password (str): A string of hashed password
        Returns:
            (str or None): The primary_key(id) of the inserted row on success.
            Otherwise, returns None
        """
        session = self._db.create_session()
        try:
            repo = UserDAL(session)
            record_id = uuid.uuid4().hex
            repo.insert(id=record_id, email=email, password=password)
            session.commit()
        except Exception as e:
            print(e)
            session.rollback()
            record_id = None
        finally:
            session.close()
            return record_id

    def update(self, id:str, email:str, password:str):
        """
        Update an existing record in the table.

        Parameters:
            id (str): The primary_key(id) of the record that needs to be modified
            email (str): User's email
            password (str): A string of hashed password
        Returns:
            int: The number of affected rows
        """
        session = self._db.create_session()
        try:
            repo = UserDAL(session)
            affected_row_count = repo.update(id, email, password)
            session.commit()
        except Exception as e:
            print(e)
            session.rollback()
            affected_row_count = 0
        finally:
            session.close()
            return affected_row_count
        
class RawDataBLL(BaseBLL):
    def _get_entity(self):
        return RawDataDAL
    
    def find_all(self) -> list[RawData] | None:
        """ Retrieve all records from the table """
        return super().find_all()

    def find_by_id(self, id) -> RawData | None:
        """ Find a single record by the given primary_key(id)

        Parameters:
            id (any): The primary_key(id)
        Returns:
            The recordset on success. Otherwise, returns None.
        """
        return super().find_by_id(id)

    def find_by_data_source_id(self, data_source_id, limit=DEFAULT_LIMIT):
        """ 
        Find all records by data_source_id and 
        limit the number of records returned based on a limit value

        Parameters:
            data_source_id (str): The id to search
            limit (int): The limit is used to specify the number of records to return. 
                            By default, limit=99999
        Returns:
            The recordset on success. Otherwise, returns None.
        """
        session = self._db.create_session()
        try:
            repo = RawDataDAL(session)
            result = repo.find_by_data_source_id(data_source_id, limit)
        except Exception as e:
            print(e)
        finally:
            session.close()
            return result
        
    def insert(self, data_source_id, data):
        """
        Insert an item into the table

        Parameters:
            id (str): The unique id of the item
            data (str): Raw data as JSON
        Returns:
            (bytes): The primary_key(id) of the inserted row on success. 
            Otherwise, returns None
        """
        session = self._db.create_session()
        try:
            repo = RawDataDAL(session)
            record_id = uuid.uuid4().bytes
            repo.insert(id=record_id, data_source_id=data_source_id, data=data)
            session.commit()
        except Exception as e:
            print(e)
            session.rollback()
            record_id = None
        finally:
            session.close()
            return record_id

    def insert_all(self, data_source_id, json_array: list[JSON]):
        """
        Insert all the items from the list into the table

        Parameters:
            data_source_id (str): Data source id
            json_array (list): A JSON array
        Returns:
            bool: True on success. Otherwise, returns False.
        """
        session = self._db.create_session()
        try:
            repo = RawDataDAL(session)
            repo.insert_all(data_source_id, json_array)
            session.commit()
            success = True
        except Exception as e:
            print(e)
            session.rollback()
            success = False
        finally:
            session.close()
            return success

    def count_if(self, data_source_id:str, since=datetime.min, until=datetime.max):
        """ 
        Count the number of records.

        Parameters:
            data_source_id (str): Data source id
            since (optional): after the given date
            until (optional): before the given date 
        Returns:
            int: The number of records
        """
        session = self._db.create_session()
        try:
            filters = {
                RawData.data_source_id == data_source_id,
                RawData.created_at >= since,
                RawData.created_at <= until
            }
            repo = RawDataDAL(session)
            result = repo.count_if(filters)
        except Exception as e:
            print(e)
            result = None
        finally:
            session.close()
            return result

class DataSourceBLL(BaseBLL):
    def _get_entity(self):
        return DataSourceDAL
    
    def find_all(self) -> list[DataSource] | None:
        """ Retrieve all records from the table """
        return super().find_all()
    
    def find_by_id(self, id) -> DataSource | None:
        """ Find a single record by the given primary_key(id)

        Parameters:
            id (any): The primary_key(id)
        Returns:
            The recordset on success. Otherwise, returns None.
        """
        return super().find_by_id(id)
    
class ScraperTaskBLL(BaseBLL):
    def _get_entity(self):
        return ScraperTaskDAL
    
    def find_all(self) -> list[ScraperTask] | None:
        """ Retrieve all records from the table """
        return super().find_all()
    
    def find_by_id(self, id) -> ScraperTask | None:
        """ Find a single record by the given primary_key(id)

        Parameters:
            id (any): The primary_key(id)
        Returns:
            The recordset on success. Otherwise, returns None.
        """
        return super().find_by_id(id)
    
    def find_by_data_source_id(self, id) -> list[ScraperTask] | None:
        """ Find a single record by the given primary_key(id)

        Parameters:
            id (any): The primary_key(id)
        Returns:
            The recordset on success. Otherwise, returns None.
        """
        session = self._db.create_session()
        try:
            repo = ScraperTaskDAL(session)
            result = repo.find_by_data_source_id(id)
        except Exception as e:
            print(e)
            result = None
        finally:
            session.close()
            return result
    
    def insert(self, data_source_id:str, description:str, query:str, 
               repeat_interval:int, enabled:bool, created_by:str):
        """
        Insert an item into the table

        Parameters:
            data_source_id (str): The id of the data souce
            description (str): The task's description
            query (str): A query string
            repeat_interval (int): The repeat interval measured in seconds
            enabled (bool): To enable or disable the task
            created_by (str): The name of the person who created the task
        Returns:
            The primary_key(id) of the inserted row on success. 
            Otherwise, returns None
        """
        session = self._db.create_session()
        try:
            repo = ScraperTaskDAL(session)
            record_id = uuid.uuid4().bytes
            repo.insert(id=record_id,
                        data_source_id=data_source_id,
                        description=description, 
                        query=query, 
                        repeat_interval=repeat_interval, 
                        enabled=enabled, 
                        created_by=created_by)
            session.commit()
        except Exception as e:
            print(e)
            session.rollback()
            record_id = None
        finally:
            session.close()
            return record_id

    def update(self, id, data_source_id:str, description:str, query:str, 
               repeat_interval:int, enabled:bool, modified_by:str):
        """
        Update an existing record in the table.

        Parameters:
            id (bytes): The primary_key(id) of the record that needs to be modified
            data_source_id (str): The id of the data souce
            description (str): The task's description
            query (str): A query string
            repeat_interval (int): The repeat interval measured in seconds
            enabled (bool): To enable or disable the task
            modified_by (str): The name of the person who modifies the task
        Returns:
            int: The number of affected rows
        """
        session = self._db.create_session()
        try:
            repo = ScraperTaskDAL(session)
            affected_row_count = repo.update(id=id, data_source_id = data_source_id,
                                                description = description, query = query,
                                                repeat_interval = repeat_interval, enabled = enabled,
                                                modified_by = modified_by, modified_at = func.now())
            session.commit()
        except Exception as e:
            print(e)
            session.rollback()
            affected_row_count = 0
        finally:
            session.close()
            return affected_row_count

    def update_last_run_time(self, id):
        """
        Update last_run_time = NOW()

        Parameters:
            id (bytes): The primary_key(id) of the record that needs to be modified.
        Returns:
            bool: True on success. Otherwise, returns False.
        """
        session = self._db.create_session()
        try:
            repo = ScraperTaskDAL(session)
            affected_row_count = repo.update(id, last_run_time=func.now())
            session.commit()
        except Exception as e:
            print(e)
            session.rollback()
            affected_row_count = 0
        finally:
            session.close()
            return affected_row_count == 1

class HashtagBLL(BaseBLL):
    def _get_entity(self):
        return HashtagDAL
    
    def insert_if_not_exists(self, hashtag:str) -> int:
        session = self._db.create_session()
        repo = HashtagDAL(session)

        # Find
        result = repo.find_by_hashtag(hashtag)
        if result:
            session.close()
            return result.id
        
        # The hashtag does not exist in DB. So, perform an INSERT
        record_id = 0
        try:
            record_id = repo.insert(hashtag)
            session.commit()
        except: # On Duplicate
            session = self._db.create_session()
            repo = HashtagDAL(session)
            result = repo.find_by_hashtag(hashtag)
            record_id = result.id
        finally:
            session.close()
            return record_id

class TopicBLL(BaseBLL):
    def _get_entity(self):
        return TopicDAL
    
    def find_by_title(self, title):
        session = self._db.create_session()
        repo = TopicDAL(session)
        return repo.find_by_title(title)

    def insert_if_not_exists(self, title:str) -> int:
        session = self._db.create_session()
        repo = TopicDAL(session)

        # Find
        result = repo.find_by_title(title)
        if result:
            session.close()
            return result.id
        
        # The hashtag does not exist in DB. So, perform an INSERT
        record_id = 0
        try:
            record_id = repo.insert(title)
            session.commit()
        except: # On Duplicate
            session = self._db.create_session()
            repo = TopicDAL(session)
            result = repo.find_by_title(title)
            record_id = result.id
        finally:
            session.close()
            return record_id
         
class TwitterUserBLL(BaseBLL):
    def _get_entity(self):
        return TwitterUserDAL
    
    def find_all(self) -> list[TwitterUser] | None:
        """ Retrieve all records from the table """
        return super().find_all()
    
    def find_by_id(self, id:int) -> TwitterUser | None:
        """ Find a single record by the given primary_key(id)

        Parameters:
            id (any): The primary_key(id)
        Returns:
            The recordset on success. Otherwise, returns None.
        """
        return super().find_by_id(id)
    
    def insert(self, record_id:int, username:str, display_name:str):
        """
        Insert an item into the table

        Parameters:
            id (str): The unique id of the user
            username (str): The username
            display_name (str): The display name
        Returns:
            The primary_key(id) of the inserted row on success. 
            Otherwise, returns None
        """
        session = self._db.create_session()
        try:
            repo = TwitterUserDAL(session)
            repo.insert(id=record_id, username=username, display_name=display_name)
            session.commit()
        except Exception as e:
            print(e)
            session.rollback()
            record_id = None
        finally:
            session.close()
            return record_id
    
    def upsert(self, id:int, username:str, display_name:str, last_updated=datetime.min) -> int:
        """
        Update or Insert an item into the table \n
        - If the given primary_key(id) doesn't exist, then this function performs an INSERT\ 
        - If primary_key(id) already exists, then this function performs an UPDATE.

        Parameters:
            id (str): The unique id of the user
            username (str): The username
            display_name (str): The display name
        Returns:
            int: The number of affected rows
        """
        session = self._db.create_session()
        try:
            repo = TwitterUserDAL(session)
            affected_row_count = repo.upsert(id, username, display_name, last_updated)
            session.commit()
        except Exception as e:
            print(e)
            session.rollback()
            affected_row_count = 0
        finally:
            session.close()
            return affected_row_count

class TweetBLL(BaseBLL):
    def _get_entity(self):
        return TweetDAL
    
    def __build_filters(self, user_id:int=None, username:str=None, 
                        since=datetime.min, until=datetime.max,
                        hashtags:list[str]=None, sentiment_score:Between = None, 
                        language:str=None):
        filters = set()

        if user_id:
            filters.add(Tweet.user_id == user_id)

        if username:
            filters.add(TwitterUser.username == username)

        if since != datetime.min:
            filters.add(Tweet.created_at >= since)
        
        if until != datetime.min:
            filters.add(Tweet.created_at <= until)
        
        if hashtags:
            filters.add(Hashtag.hashtag.in_(hashtags))
            
        if sentiment_score:
            filters.add(Tweet.sentiment_score >= sentiment_score.low)
            filters.add(Tweet.sentiment_score <= sentiment_score.high)

        if language:
            filters.add(Tweet.language == language)
        
        return filters

    def find_all(self) -> list[Tweet] | None:
        """ Retrieve all records from the table """
        return super().find_all()
    
    def find_by_id(self, id:int) -> Tweet | None:
        """ Find a single record by the given primary_key(id)

        Parameters:
            id (any): The primary_key(id)
        Returns:
            The recordset on success. Otherwise, returns None.
        """
        return super().find_by_id(id)
    
    def filter_by(self, user_id:int=None, username:str=None, 
               since=datetime.min, until=datetime.max,
               hashtags:list[str]=None, sentiment_score:Between = None, 
               language:str=None, exclude_content=False) -> list[Tweet]:
        """ 
        Filter records by criteria. Multiple criteria may be specified as comma separated; 
        the effect is that they will be joined together using the and_() function.
        >>> repo = TweetBLL(db)
            # Get all tweets by Elon Musk.
            result = repo.filter_by(username='elonmusk')
            # - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # Get all tweets BY `Elon Musk` 
            #                WITH hashtags: `tesla or spacex`.
            #                AND do not load tweet content
            result = repo.filter_by(username='elonmusk', 
                                    hashtags=['tesla', 'spacex'],
                                    exclude_content=True)
            # - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # Get all tweets BY `Elon Musk` 
            #                IN 2023 IN English
            #                WITH hashtags: `tesla or spacex`
            #                AND sentiment_score > 30
            result = repo.filter_by(username='elonmusk',
                                    since='2023-01-01', 
                                    until='2023-12-31 23:59:59',
                                    language='en',
                                    sentiment_score=Between(30, 100),
                                    hashtags=['tesla', 'spacex'])

        Parameters:
            user_id (int): Twitter user id
            username (str): Twitter username
            since (datetime): from the given date
            until (datetime): up to the given date
            sentiment_score (Between): filter by the sentiment scores within a given range Between(min, max)
            hashtags (list[str]): THIS FEATURE WILL BE IMPLEMENTED IN THE NEXT UPDATE.
            language (str): The language of the tweet
            exclude_content (bool): Avoid loading the 'Tweet Content' when it's not needed.
        Returns:
            A list of records belong to the user
        """
        session = self._db.create_session()
        try:
            filters = self.__build_filters(user_id, username, since, until,
                                           hashtags, sentiment_score, language)
            repo = TweetDAL(session)
            result = repo.filter(filters, exclude_content=exclude_content)
        except Exception as e:
            print(e)
        finally:
            session.close()
            return result

    def count_if(self, user_id:int=None, username:str=None, 
               since=datetime.min, until=datetime.max,
               hashtags:list[str]=None, sentiment_score:Between = None, 
               language:str=None) -> int:
        """
        Count the number of records.

        Parameters:
            user_id (int): filter by Twitter user id
            since (datetime): from the given date
            until (datetime): up to the given date
            sentiment_score (Between): filter by the sentiment scores within a given range Between(min, max)
        Returns:
            int: The number of records
        """
        session = self._db.create_session()
        try:
            filters = self.__build_filters(user_id, username, since, until,
                                           hashtags, sentiment_score, language)
            repo = TweetDAL(session)
            result = repo.count_if(filters)
        except Exception as e:
            print(e)
            result = 0
        finally:
            session.close()
            return result
            
    def upsert(self, tweet_id:int, twitter_user_id:int, username:str, display_name:str,
               content:str, language:str | None, created_at:datetime, sentiment_score:int | None,
               like_count:int, retweet_count:int, reply_count:int, topics:list[str] = None) -> int:
        """
        Update or Insert an item into the table

        - If the given primary_key(id) doesn't exist, then this function performs an INSERT 
        - If primary_key(id) already exists, then this function performs an UPDATE.

        Parameters:
            tweet_id (int): The unique id of the tweet
            twitter_user_id (int): The unique id of the user
            username (str): The username of the user
            display_name (str): The display name for the user
            content (str): The content of the tweet
            language (str | None): The language of the content
            created_at (str): Tweet's posted date
            sentiment_score (int | None): Sentiment score can be a NULL or an Integer from -100 to 100
            like_count (int): Number of likes
            retweet_count (int): Number of retweets
            reply_count (int): Number of relies
            topics (list[str]): List of topics

        Returns:
            int: The number of affected rows
        """
        session = self._db.create_session()
        try:
            ###########################################################
            #       Validate & Format data before sending to DB       #
            ###########################################################

            # ------------------ Tweet Content ------------------
            # 1. Normal Twitter user can write up to 280 characters.
            # 2. Twitter Blue subscribers can write 4,000-character tweets.
            # 3. 99.99% of tweets are less than 280 characters.
            # 4. The database was designed to have a 280-char limit.
            # So, because of above reasons, we need to truncate a long string.
            if content != None and len(content) > 280:
                content = content[:280]
            
            # --------- The language ISO code Max Length = 5 ---------
            # E.g   Input           | Output | Language Name
            #       ------------------------------------------------
            #       en              | en     | English
            #       zh-Hans         | zh-Ha  | Chinese (Simplified) (zh-CHS)
            #       zh-Hant         | zh-Ha  | Chinese (Simplified) (zh-CHT)
            #       zh-HK           | zh-HK  | Chinese (Traditional, Hong Kong S.A.R.)
            #       ca-ES-valencia  | ca-ES  | Valencian (Spain)
            if language != None and len(language) > 5:
                language = language[:5]

            # UPDATE or INSERT a `twitter user`
            repo = TwitterUserDAL(session)
            repo.upsert(twitter_user_id, username, display_name, created_at)

            # UPDATE or INSERT a `tweet`
            repo = TweetDAL(session)
            affected_row_count = repo.upsert(tweet_id, twitter_user_id, content, 
                                             language, created_at, sentiment_score, 
                                             like_count, retweet_count, reply_count)
            session.commit()
        except Exception as e:
            print(e)
            session.rollback()
            affected_row_count = 0
        finally:
            session.close()
            
             # INSERT INTO `hashtag`
            hashtags = set(re.findall('#(\w+)', content))
            repo = HashtagBLL(self._db)
            hashtag_ids = list[int]()

            for e in hashtags:
                hashtag_id = repo.insert_if_not_exists(e)
                hashtag_ids.append(hashtag_id)
                
            # INSERT INTO `tweet_hashtag`
            repo = TweetHashtagBLL(self._db)
            repo.insert(tweet_id, hashtag_ids)

            ######## TOPIC ############
            if topics:
                # INSERT INTO `topic`
                topic_ids = set[int]()
                title_max_length = Topic.title.property.columns[0].type.length
                repo = TopicBLL(self._db)
                for e in topics:
                    if len(e) > title_max_length:
                        e = e[:title_max_length]
                    topic_id = repo.insert_if_not_exists(e)
                    topic_ids.add(topic_id)

                # INSERT INTO `tweet_topic`
                repo = TweetTopicBLL(self._db)
                repo.insert(tweet_id, topic_ids)

            return affected_row_count
    
    def delete_all(self) -> int:
        session = self._db.create_session()
        try:
            # DELETE FROM tweet_hashtag;
            # repo = TweetHashtagDAL(session)
            # repo.delete_all()

            # DELETE FROM tweet;
            repo = TweetDAL(session)
            affected_row_count = repo.delete_all()

            # DELETE FROM hashtag;
            repo = HashtagDAL(session)            
            repo.delete_all()
            
            session.commit()
        except Exception as e:
            print(e)
            session.rollback()
            affected_row_count = 0
        finally:
            session.close()
            return affected_row_count
        
class TweetHashtagBLL(BaseBLL):
    def _get_entity(self):
        return TweetHashtagDAL
    
    def insert(self, tweet_id:int, hashtag_id_list:list[int]) -> int:
        session = self._db.create_session()
        success:bool
        try:
            repo = TweetHashtagDAL(session)
            repo.delete_by_tweet_id(tweet_id)
            hashtags_in_tweet = list[TweetHashtag]()
            
            for hashtag_id in hashtag_id_list:
                hashtags_in_tweet.append(TweetHashtag(tweet_id, hashtag_id))

            session.add_all(hashtags_in_tweet)
            success = True
            session.commit()
        except:
            success = False
        finally:
            session.close()
            return success
        
class TweetTopicBLL(BaseBLL):
    def _get_entity(self):
        return TweetTopicDAL
    
    def insert(self, tweet_id:int, topic_id_list:list[int]) -> int:
        session = self._db.create_session()
        success:bool
        try:
            repo = TweetTopicDAL(session)
            repo.delete_by_tweet_id(tweet_id)
            topics_in_tweet = list[TweetTopic]()
            
            for index, topic_id in enumerate(topic_id_list):
                topics_in_tweet.append(TweetTopic(tweet_id, topic_id, index))

            session.add_all(topics_in_tweet)
            success = True
            session.commit()
        except:
            success = False
        finally:
            session.close()
            return success