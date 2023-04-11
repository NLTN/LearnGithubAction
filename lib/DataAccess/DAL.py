from abc import ABC, abstractmethod
from sqlalchemy.sql import func
from sqlalchemy import select, update, delete, case, ColumnElement
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import Session, defer
from models import *
import uuid

DEFAULT_LIMIT = 99999

class BaseDAL(ABC):
    def __init__(self, session):
        self.session:Session = session
    
    def find_all(self):
        """ Retrieve all records from the table """
        Entity = self._get_entity()
        return self.session.query(Entity).all()
    
    def filter(self, filters = {}):
        Entity = self._get_entity()
        stmt = select(Entity).filter(*filters)
        result = self.session.execute(stmt)
        return result.scalars().all()
    
    def find_by_id(self, id):
        """ Find a single record by the given primary_key(id)

        Parameters:
            id (any): The primary_key(id)
        Returns:
            The recordset on success. Otherwise, returns None.
        """
        Entity = self._get_entity()
        stmt = select(Entity).where(Entity.id == id)
        result = self.session.execute(stmt).fetchone()
        return result[0] if result else None
    
    def insert(self, **kwvalues):
        """ 
        Insert a record into the table

        Parameters:
            kwvalues (dict): A dictionary of values
        Returns:
            Any: An object representing results of the statement execution
        """
        Entity = self._get_entity()
        stmt = insert(Entity).values(kwvalues)
        result = self.session.execute(stmt)
        return result
    
    def insert_all(self, entities:list[any]):
        """
        Insert all the items from the list into the table

        Parameters:
            entities (list): List of entities
        """
        self.session.add_all(entities)

    def delete(self, id) -> int:
        """ Delete a row by id

        Parameters:
            id (any): The primary_key(id)
        Returns:
            int: The number of affected rows
        """
        try:
            Entity = self._get_entity()
            stmt = delete(Entity).where(Entity.id==id)
            result = self.session.execute(stmt)
        except Exception as e:
            print(e)
        finally:
            return result.rowcount
    
    def delete_all(self) -> int:
        """ Delete all records from the table
        
        Returns:
            int: The number of affected rows
        """
        try:
            Entity = self._get_entity()
            affected_row_count = self.session.query(Entity).delete()
            return affected_row_count
        except Exception as e:
            print(e)
    
    def count_if(self, filters={}) -> int:
        """ 
        Count the number of records. Example:
        
        >>> filters = {
                        user_id == 12345,
                        created_at > '2023-03-23'
                      }
            row_count = repo.count_if(filters)

        Parameters:
            filters (tuple[ExpressionArgument[bool]): Filter criteria that will be added to
                    the WHERE clause
        Returns:
            int: The number of records
        """
        Entity = self._get_entity()
        stmt = select(func.count()).select_from(Entity).filter(*filters)
        result = self.session.execute(stmt)
        return result.scalar_one()

    @abstractmethod
    def _get_entity(self):
        pass

    @abstractmethod
    def update(self):
        pass

class UserDAL(BaseDAL):
    def _get_entity(self):
        return User
    
    def find_by_email(self, email) -> User | None:
        """ Find a single record by the given email

        Parameters:
            email (any): The user's email
        Returns:
            The recordset on success. Otherwise, returns None.
        """
        Entity = self._get_entity()
        stmt = select(Entity).where(Entity.email == email)
        result = self.session.execute(stmt)
        return result.scalars().first()

    def update(self, id:str, email:str, password:str) -> int:
        Entity = self._get_entity()
        stmt = update(Entity).where(Entity.id==id).values(email=email, password=password)
        result = self.session.execute(stmt)
        return result.rowcount

class DataSourceDAL(BaseDAL):
    def _get_entity(self):
        return DataSource

    def update(self):
        raise NotImplemented()

class RawDataDAL(BaseDAL):
    def _get_entity(self):
        return RawData
    
    def find_by_data_source_id(self, data_source_id, limit=DEFAULT_LIMIT) -> list[RawData]:
        """ 
        Find all records by data_source_id

        Parameters:
            data_source_id (str): The id to search
            limit (int): The limit is used to specify the number of records to return. By default, limit=99999
        Returns:
            The recordset on success. Otherwise, returns None.
        """
        Entity = self._get_entity()
        stmt = select(Entity).where(Entity.data_source_id == data_source_id).limit(limit)
        result = self.session.execute(stmt)
        return result.scalars().all()
    
    def insert_all(self, data_source_id, json_array: list[JSON]):
        """
        Insert all the items from the list into the table

        Parameters:
            id (str): The unique id of the item
            items (list): A JSON array
        """
        Entity = self._get_entity()
        values = []
        for e in json_array:
            values.append(Entity(uuid.uuid4().bytes, data_source_id, e, None))
        self.session.add_all(values)
    
    def update(self):
        raise NotImplemented()
    
class ScraperTaskDAL(BaseDAL):
    def _get_entity(self):
        return ScraperTask
    
    def find_by_data_source_id(self, data_source_id) -> list[ScraperTask]:
        """ 
        Find all records by data_source_id

        Parameters:
            data_source_id (str): The id to search
        Returns:
            The recordset on success. Otherwise, returns None.
        """
        Entity = self._get_entity()
        stmt = select(Entity).where(Entity.data_source_id==data_source_id)
        result = self.session.execute(stmt)
        return result.scalars().all()

    def update(self, id:bytes, **kwvalues):
        Entity = self._get_entity()
        kwvalues['modified_at']=func.now()
        stmt = update(Entity).where(Entity.id==id).values(kwvalues)
        result = self.session.execute(stmt)
        return result.rowcount

class HashtagDAL(BaseDAL):
    def _get_entity(self):
        return Hashtag
    
    def find_by_hashtag(self, value) -> Hashtag | None:
        stmt = select(Hashtag).where(Hashtag.hashtag == value)
        result = self.session.execute(stmt)
        return result.scalar_one_or_none()
    
    def insert(self, hashtag:str):
        """
        Insert a hashtag into database if not exists.

        >>> repo.insert('BTC')
            OR
            repo.insert('#BTC')

        Parameters:
            hashtag (str): The hashtag
        
        Returns:
            (int): The id of the inserted hashtag.
        """
        value = hashtag.replace('#', '')
        
        item = Hashtag(hashtag=value)
        self.session.add(item)
        self.session.flush()
        return item.id
    
        # sub_stmt_2 = select(text('1')).select_from(Hashtag) \
        #                             .where(Hashtag.hashtag == value) \
        #                             .exists()
        # sub_stmt_1 = select(text(f'"{value}"')).select_from(text('DUAL')) \
        #                             .where(~sub_stmt_2) \
        #                             .limit(1)
        # stmt = insert(Hashtag).from_select(['hashtag'], sub_stmt_1)
        # # ---------- equivalent to ----------
        # # INSERT INTO hashtag (hashtag)
        # # SELECT "bitcoin" 
        # # FROM DUAL 
        # # WHERE NOT (EXISTS (SELECT 1 
        # #                    FROM hashtag 
        # #                    WHERE hashtag.hashtag = "bitcoin"))
        # # LIMIT 1;
        # result = self.session.execute(stmt)
        # return result.rowcount
    
    # def update_use_count(self, hashtag:str, step: int):
    #     stmt = update(Hashtag).values({Hashtag.use_count.key:Hashtag.use_count + step}) \
    #                           .where(Hashtag.hashtag == hashtag)
    #     result = self.session.execute(stmt)
    #     return result.rowcount
    
    def upsert(self, hashtag:str, use_count_increment:int=0):
        """
        Update or Insert an item into the table
        
        - If the given primary_key(id) doesn't exist, then this function performs an INSERT 
        - If primary_key(id) already exists, then this function performs an UPDATE.

        >>> # Upsert 'BTC' hashtag into DB and INCREASE use_count by 1 if the hashtag already exists.
            repo.upsert('BTC', 1)
            #
            # Upsert 'BTC' hashtag into DB and DECREASE use_count by 1 if the hashtag already exists.
            repo.upsert('BTC', -1)

        Parameters:
            hashtag (str): The hashtag
            use_count_increment (int): Increase use_count by a value

        Returns:
            int: The number of affected rows
        """
        stmt = insert(Hashtag).values({
                Hashtag.hashtag.key:hashtag,
                Hashtag.use_count.key:use_count_increment if use_count_increment >= 0 else 0
            })
        stmt = stmt.on_duplicate_key_update({ 
                Hashtag.use_count.key:Hashtag.use_count + use_count_increment 
            })
        result = self.session.execute(stmt)
        return result.rowcount
        
    def update(self):
        raise NotImplemented()
    
class TopicDAL(BaseDAL):
    def _get_entity(self):
        return Topic
    
    def find_by_title(self, title) -> Topic | None:
        """
        Find a topic by tilte.

        >>> repo.find_by_title('title 1')

        Parameters:
            title (str): Topic title
        
        Returns:
            (Topic): The topic record.
        """
        stmt = select(Topic).where(Topic.title == title)
        result = self.session.execute(stmt)
        return result.scalar_one_or_none()
    
    def insert(self, title:str):
        """
        Insert a topic into database.

        >>> repo.insert('BTC')

        Parameters:
            title (str): Topic title
        
        Returns:
            (int): The id of the inserted topic.
        """
        item = Topic(title=title)
        self.session.add(item)
        self.session.flush()
        return item.id
    
    def update(self):
        raise NotImplemented()
    
class TwitterUserDAL(BaseDAL):
    def _get_entity(self):
        return TwitterUser

    def upsert(self, id:int, username:str, display_name:str, last_updated=datetime.min) -> int:
        """
        If the given primary_key(id) doesn't exist, then this function performs an INSERT\ 
        If primary_key(id) already exists, then this function performs an UPDATE.
        
        Parameters:
            id (str): The unique id of the user
            username (str): The username
            display_name (str): The display name
        Returns:
            (int): The number of affected rows
        """
        Entity = self._get_entity()
        insert_values = {
                    TwitterUser.id.key:id,
                    TwitterUser.username.key:username,
                    TwitterUser.display_name.key:display_name,
                    TwitterUser.last_updated.key:last_updated
                  }
        stmt = insert(Entity).values(insert_values)

        new_values = {
            TwitterUser.username.key:case((TwitterUser.last_updated < last_updated, username),
                                            else_=TwitterUser.username),
            TwitterUser.display_name.key:case((TwitterUser.last_updated < last_updated, display_name),
                                            else_=TwitterUser.display_name),
            TwitterUser.last_updated.key:case((TwitterUser.last_updated < last_updated, last_updated),
                                                else_=TwitterUser.last_updated)
            }
        stmt = stmt.on_duplicate_key_update(new_values)
        
        result = self.session.execute(stmt)
        return result.rowcount

    def update(self):
        raise NotImplemented()

class TweetDAL(BaseDAL):
    def _get_entity(self):
        return Tweet
    
    def filter(self, filters:set[ColumnElement[bool]]={}, exclude_content=False, exclude_user_info=False) -> list[Tweet]:
        """
        Filter records by criteria. E.g:

        >>> filters = { Tweet.user_id == 12345,
                        Tweet.created_at >= '2023-03-23',
                        Tweet.created_at <= '2023-04-01'}
            records = repo.filter(filters)
            for e in records:
                print(e)

        Parameters:
            filters (tuple[ExpressionArgument[bool]): Filter criteria that will be added to
                    the WHERE clause
            exclude_content (bool): Avoid loading the 'Tweet Content' when it's not needed.
            exclude_user_info (bool): Avoid loading user info such as username or display_name
        Returns:
            A list of records
        """
        # TODO: Optimize this line of code
        has_hashtag_filters = True in [True for e in filters if e.left.key == Hashtag.hashtag.key]
        
        # Build SQL query
        if has_hashtag_filters:
            stmt = select(Tweet, TwitterUser).select_from(Tweet, TwitterUser, TweetHashtag, Hashtag).options(defer(Tweet.content, raiseload=False))\
                    .distinct() \
                    .where(Tweet.user_id == TwitterUser.id, 
                           Tweet.id == TweetHashtag.tweet_id,
                           Hashtag.id == TweetHashtag.hashtag_id) \
                    .where(*filters)
        else:
            stmt = select(Tweet, TwitterUser).options(defer(Tweet.content, raiseload=False))\
                    .where(Tweet.user_id == TwitterUser.id).where(*filters)
        
        # Execute
        result = self.session.execute(stmt)
        data = list[Tweet]()
        
        for t, u in result.fetchall():
            tw:Tweet = t
            tw_user:TwitterUser = u
            tw_user.__dict__.pop('_sa_instance_state', None)
            tweet = Tweet(tw.id, tw.user_id, 
                        tw.content if not exclude_content else None, 
                        tw.language, 
                        tw.created_at, tw.sentiment_score, tw.like_count, 
                        tw.retweet_count, tw.reply_count, user=tw_user.__dict__)
            data.append(tweet)
        return data
    
    def count_if(self, filters={}) -> int:
        """ 
        Count the number of records. Example:
        
        >>> filters = {
                        user_id == 12345,
                        created_at > '2023-03-23'
                      }
            row_count = repo.count_if(filters)

        Parameters:
            filters (tuple[ExpressionArgument[bool]): Filter criteria that will be added to
                    the WHERE clause
        Returns:
            int: The number of records
        """
        stmt = select(func.count()).select_from(Tweet, TwitterUser)\
                                    .where(Tweet.user_id == TwitterUser.id).filter(*filters)
        result = self.session.execute(stmt)
        return result.scalar_one()

    def upsert(self, tweet_id:int, twitter_user_id:int, content:str, language:str, created_at: str,
                sentiment_score: int,like_count:int, retweet_count:int, reply_count:int) -> int:
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
            language (str): The language of the content
            created_at (str): Tweet's posted date
            sentiment_score (int | None): Sentiment score can be NULL or in range from -100 to 100
            like_count (int): Number of likes
            retweet_count (int): Number of retweets
            reply_count (int): Number of relies

        Returns:
            int: The number of affected rows
        """
        Entity = self._get_entity()
        stmt = insert(Entity).values(id=tweet_id, 
                                     user_id = twitter_user_id, 
                                     content = content,
                                     language = language, 
                                     created_at = created_at,
                                     sentiment_score = sentiment_score, 
                                     like_count = like_count,
                                     retweet_count = retweet_count, 
                                     reply_count = reply_count)
        
        stmt = stmt.on_duplicate_key_update(
            content = content,
            language = language,
            created_at = created_at,
            sentiment_score = sentiment_score, 
            like_count = like_count,
            retweet_count = retweet_count,
            reply_count = reply_count
        )
        result = self.session.execute(stmt)
        return result.rowcount

    def update(self):
        raise NotImplemented()
    
class TweetHashtagDAL(BaseDAL):
    def _get_entity(self):
        return TweetHashtag
    
    # def insert(self, tweet_id:int, hashtag:str):
    #     """
    #     Insert a hashtag into database if not exists.

    #     >>> repo.insert(12345, 'BTC')
    #         OR
    #         repo.insert(12345, '#BTC')

    #     Parameters:
    #         tweet_id (int): Tweet id
    #         hashtag (str): The hashtag

    #     Returns:
    #         (int): The number of affected rows 
    #     """
    #     get_hashtag_id_stmt = select(Hashtag.id).where(Hashtag.hashtag == hashtag)
        
    #     # [DEADLOCK]
    #     stmt = insert(TweetHashtag).values({TweetHashtag.tweet_id.key:tweet_id,
    #                                         TweetHashtag.hashtag_id.key:get_hashtag_id_stmt.scalar_subquery()}) \
    #                    .prefix_with('IGNORE')
    #     result = self.session.execute(stmt)
    #     return result.rowcount
    
    def insert(self, tweet_id:int, hashtag_id:int):
        """
        Link a hashtag to a tweet 

        >>> repo.insert(12345, 9999)

        Parameters:
            tweet_id (int): Tweet id
            hashtag_id (str): The hashtag_id

        Returns:
            (int): The number of affected rows 
        """
        stmt = insert(TweetHashtag).values({TweetHashtag.tweet_id.key:tweet_id,
                                            TweetHashtag.hashtag_id.key:hashtag_id}) \
                       .prefix_with('IGNORE')
        result = self.session.execute(stmt)
        return result.rowcount
    
    def delete(self, tweet_id:int, hashtag:str):
        """
        Delete a hashtag in a tweet.

        >>> repo.delete(12345, 'BTC')
            OR
            repo.delete(12345, '#BTC')

        Parameters:
            tweet_id (int): Tweet id
            hashtag (str): The hashtag
        
        Returns:
            (int): The number of affected rows 
        """
        get_hashtag_id_stmt = select(Hashtag.id).where(Hashtag.hashtag == hashtag)
        stmt = delete(TweetHashtag).where(TweetHashtag.tweet_id == tweet_id,
                                          TweetHashtag.hashtag_id == get_hashtag_id_stmt.scalar_subquery())
        result = self.session.execute(stmt)
        return result.rowcount
    
    def delete_by_tweet_id(self, tweet_id:int):
        """
        Delete by tweet_id.

        >>> repo.delete_by_tweet_id(12345)

        Parameters:
            tweet_id (int): Tweet id
        
        Returns:
            (int): The number of affected rows 
        """
        stmt = delete(TweetHashtag).where(TweetHashtag.tweet_id == tweet_id)
        result = self.session.execute(stmt)
        return result.rowcount
    
    def delete_if_not_in_list(self, tweet_id, hashtags: list[str]):
        # [DEADLOCK] Update hashtag's use_count
        # stmt = update(Hashtag).values({Hashtag.use_count.key:Hashtag.use_count - 1}) \
        #                     .where(Hashtag.id == TweetHashtag.hashtag_id) \
        #                     .where(TweetHashtag.tweet_id == tweet_id,
        #                             Hashtag.hashtag.in_(hashtags))
        # self.session.execute(stmt)
        
        # Delete from TweetHashtag
        get_hashtag_ids = select(Hashtag.id).where(Hashtag.hashtag.in_(hashtags))
        stmt = delete(TweetHashtag).where(TweetHashtag.tweet_id == tweet_id,
                                          TweetHashtag.hashtag_id.not_in(get_hashtag_ids.scalar_subquery()))
        result = self.session.execute(stmt)
        return result.rowcount

    def update(self):
        raise NotImplemented()
    
class TweetTopicDAL(BaseDAL):
    def _get_entity(self):
        return TweetTopic
    
    def delete_by_tweet_id(self, tweet_id:int):
        """
        Delete by tweet_id.

        >>> repo.delete_by_tweet_id(12345)

        Parameters:
            tweet_id (int): Tweet id
        
        Returns:
            (int): The number of affected rows 
        """
        stmt = delete(TweetTopic).where(TweetTopic.tweet_id == tweet_id)
        result = self.session.execute(stmt)
        return result.rowcount
    
    def update(self):
        raise NotImplemented()