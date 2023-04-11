from sqlalchemy.sql import func
from sqlalchemy import Column, CHAR, SMALLINT, Integer, BIGINT, BOOLEAN, DATETIME, String, BINARY, JSON, ForeignKey, text
from sqlalchemy.orm import Mapped, registry, relationship
from datetime import datetime
import uuid

reg = registry()
Base = reg.generate_base()

@reg.mapped_as_dataclass
class User:
    __tablename__ = 'user'
    
    id:Mapped[str] = Column(String(32), primary_key=True, nullable=False, default=uuid.uuid4().hex)
    email:Mapped[str] = Column(String(255), unique=True, nullable=False)
    password:Mapped[str] = Column(String(64), nullable=False)

@reg.mapped_as_dataclass
class DataSource:
    # __table_args__ = {'schema': 'StressTest'}
    __tablename__ = 'data_source'

    id:Mapped[str] = Column(CHAR(3), primary_key=True)
    name:Mapped[str] = Column(String(50), nullable=False)

@reg.mapped_as_dataclass
class RawData:
    __tablename__ = 'raw_data'
    
    id:Mapped[bytes] = Column(BINARY(16), primary_key=True)
    data_source_id:Mapped[str] = Column(CHAR(3), ForeignKey('data_source.id'), nullable=False)
    data:Mapped[str] =  Column(JSON, nullable=False)
    created_at:Mapped[datetime] = Column(DATETIME, nullable=False, server_default=func.now())

@reg.mapped_as_dataclass
class ScraperTask:
    __tablename__ = 'scraper_task'

    id:Mapped[bytes] = Column(BINARY(16), primary_key=True)
    data_source_id:Mapped[str] = Column(CHAR(3), ForeignKey('data_source.id'), nullable=False)
    description:Mapped[str] =  Column(String(255), nullable=False)
    query:Mapped[str] =  Column(String(1024), nullable=False)
    repeat_interval:Mapped[int] = Column(Integer, nullable=False)
    enabled:Mapped[bool] = Column(BOOLEAN, nullable=False)
    last_run_time:Mapped[datetime] = Column(DATETIME, nullable=True)
    created_by:Mapped[str] = Column(String(50), nullable=True)
    created_at:Mapped[datetime] = Column(DATETIME, nullable=False, server_default=func.now())
    modified_by:Mapped[str] = Column(String(50), nullable=True)
    modified_at:Mapped[datetime] = Column(DATETIME, nullable=True)

@reg.mapped_as_dataclass
class Hashtag:
    __tablename__ = 'hashtag'
    
    id:Mapped[int] = Column(BIGINT, primary_key=True, nullable=False, autoincrement=True)
    hashtag:Mapped[str] = Column(String(100), nullable=False, index=True, unique=True)
    use_count:Mapped[int] = Column(BIGINT, nullable=False, server_default='0')
    tweets = relationship('Tweet', secondary='tweet_hashtag', viewonly=True)

@reg.mapped_as_dataclass
class Topic:
    __tablename__ = 'topic'
    
    id:Mapped[int] = Column(BIGINT, primary_key=True, nullable=False, autoincrement=True)
    title:Mapped[str] = Column(String(100), nullable=False, index=True, unique=True)
    use_count:Mapped[int] = Column(BIGINT, nullable=False, server_default='0')
    tweets = relationship('Tweet', secondary='tweet_topic', viewonly=True)

@reg.mapped_as_dataclass
class TwitterUser:
    __tablename__ = 'twitter_user'
    
    id:Mapped[int] = Column(BIGINT, primary_key=True, nullable=False, autoincrement=False)
    username:Mapped[str] = Column(String(50), nullable=False, index=True, unique=True)
    display_name:Mapped[str] = Column(String(100), nullable=False)
    last_updated:Mapped[datetime] = Column(DATETIME, nullable=False, server_default='01-01-01 00:00:00')

@reg.mapped_as_dataclass
class Tweet:
    __tablename__ = 'tweet'
    
    id:Mapped[int] = Column(BIGINT, primary_key=True, nullable=False, autoincrement=False)
    user_id:Mapped[int] = Column(BIGINT, ForeignKey('twitter_user.id'), nullable=False)
    content:Mapped[str] = Column(String(280), nullable=False)
    language:Mapped[str] =  Column(CHAR(5), nullable=True)
    created_at:Mapped[datetime] = Column(DATETIME, nullable=False)
    sentiment_score:Mapped[int] = Column(SMALLINT, nullable=True)
    like_count:Mapped[int] = Column(Integer, nullable=False, default=0)
    retweet_count:Mapped[int] = Column(Integer, nullable=False, default=0)
    reply_count:Mapped[int] = Column(Integer, nullable=False, default=0)
    hashtags = relationship('Hashtag', secondary='tweet_hashtag', cascade="all, delete")
    
    # --- EXTRA MEMBER VARIABLES ---
    # The variables below are for application layer only.
    # They don't exist in database.
    user:dict = None

@reg.mapped_as_dataclass
class TweetHashtag:
    __tablename__ = 'tweet_hashtag'
    
    tweet_id:Mapped[int] = Column(BIGINT, ForeignKey('tweet.id', onupdate="CASCADE", ondelete="CASCADE"), primary_key=True)
    hashtag_id:Mapped[int] = Column(BIGINT, ForeignKey('hashtag.id'), primary_key=True)

@reg.mapped_as_dataclass
class TweetTopic:
    __tablename__ = 'tweet_topic'
    
    tweet_id:Mapped[int] = Column(BIGINT, ForeignKey('tweet.id', onupdate="CASCADE", ondelete="CASCADE"), primary_key=True)
    topic_id:Mapped[int] = Column(BIGINT, ForeignKey('topic.id'), primary_key=True)
    sort_order:Mapped[int] = Column(SMALLINT, server_default='0')