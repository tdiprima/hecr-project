from sqlalchemy import DECIMAL, Column, ForeignKey, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True)
    email = Column(String(255))
    firstname = Column(String(100))
    lastname = Column(String(100))
    middlename = Column(String(100))
    employmentstatus = Column(String(50))
    position = Column(String(100))
    primaryunit = Column(Integer)
    orcid = Column(String(50))
    rank = Column(String(100))
    url = Column(Text)
    lastlogin = Column(String(50))
    pid = Column(Integer)

    publications = relationship("Publication", back_populates="user")
    grants = relationship("Grant", back_populates="user")


class Publication(Base):
    __tablename__ = "publications"

    id = Column(Integer, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"))
    activityid = Column(Integer, unique=True)
    type = Column(String(50))
    title = Column(Text)
    journal = Column(String(255))
    series_title = Column(String(255))
    year = Column(Integer)
    month_season = Column(String(50))
    publisher = Column(String(255))
    publisher_city_state = Column(String(255))
    publisher_country = Column(String(100))
    volume = Column(String(50))
    issue_number = Column(String(50))
    page_numbers = Column(String(100))
    isbn = Column(String(50))
    issn = Column(String(50))
    doi = Column(String(255))
    url = Column(Text)
    description = Column(Text)
    origin = Column(String(50))
    status = Column(String(50))
    term = Column(String(50))
    status_year = Column(Integer)

    user = relationship("User", back_populates="publications")


class Grant(Base):
    __tablename__ = "grants"

    id = Column(Integer, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"))
    activityid = Column(Integer, unique=True)
    title = Column(Text)
    sponsor = Column(String(255))
    grant_id = Column(String(100))
    award_date = Column(String(20))
    start_date = Column(String(20))
    end_date = Column(String(20))
    period_length = Column(Integer)
    period_unit = Column(String(20))
    indirect_funding = Column(DECIMAL(15, 2))
    indirect_cost_rate = Column(String(20))
    total_funding = Column(DECIMAL(15, 2))
    total_direct_funding = Column(DECIMAL(15, 2))
    currency_type = Column(String(10))
    description = Column(Text)
    abstract = Column(Text)
    number_of_periods = Column(Integer)
    url = Column(Text)
    status = Column(String(50))
    term = Column(String(50))
    status_year = Column(Integer)

    user = relationship("User", back_populates="grants")
