import os

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Float, NVARCHAR
from sqlalchemy import ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship


Base = declarative_base()


def make_engine():
    host = os.environ["DB_HOST"]
    port = 1433
    database = os.environ["DB_NAME"]
    username = "SA"
    password = os.environ["DB_PASS"]
    connect_url = sqlalchemy.engine.url.URL(
        "mssql+pyodbc",
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
        query=dict(driver="ODBC Driver 17 for SQL Server"),
    )
    engine = create_engine(connect_url)
    return engine


def make_session():
    engine = make_engine()
    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()
    return session


def make_database():
    Base.metadata.create_all(make_engine())


class MedicationIssue(Base):
    __tablename__ = "MedicationIssue"

    Patient_ID = Column(Integer, ForeignKey("Patient.Patient_ID"))
    Patient = relationship("Patient", back_populates="MedicationIssues")
    Consultation_ID = Column(Integer)
    MedicationIssue_ID = Column(Integer, primary_key=True)
    RepeatMedication_ID = Column(Integer)
    MultilexDrug_ID = Column(
        NVARCHAR(length=20), ForeignKey("MedicationDictionary.MultilexDrug_ID")
    )
    MedicationDictionary = relationship(
        "MedicationDictionary", back_populates="MedicationIssues", cascade="all, delete"
    )
    Dose = Column(String)
    Quantity = Column(String)
    StartDate = Column(DateTime)
    EndDate = Column(DateTime)
    MedicationStatus = Column(String)
    ConsultationDate = Column(String)


class MedicationDictionary(Base):
    __tablename__ = "MedicationDictionary"

    MultilexDrug_ID = Column(NVARCHAR(length=20), primary_key=True)
    MedicationIssues = relationship(
        "MedicationIssue", back_populates="MedicationDictionary"
    )
    ProductId = Column(String)
    FullName = Column(String)
    RootName = Column(String)
    PackDescription = Column(String)
    Form = Column(String)
    Strength = Column(String)
    CompanyName = Column(String)
    DMD_ID = Column(String)


class CodedEvent(Base):
    __tablename__ = "CodedEvent"

    Patient_ID = Column(Integer, ForeignKey("Patient.Patient_ID"))
    Patient = relationship(
        "Patient", back_populates="CodedEvents", cascade="all, delete"
    )
    CodedEvent_ID = Column(Integer, primary_key=True)
    CTV3Code = Column(String)
    NumericValue = Column(Float)
    ConsultationDate = Column(DateTime)
    SnomedConceptId = Column(String)


class Patient(Base):
    __tablename__ = "Patient"
    Patient_ID = Column(Integer, primary_key=True)
    DateOfBirth = Column(DateTime)
    DateOfDeath = Column(DateTime)
    MedicationIssues = relationship(
        "MedicationIssue",
        back_populates="Patient",
        cascade="all, delete, delete-orphan",
    )
    CodedEvents = relationship(
        "CodedEvent", back_populates="Patient", cascade="all, delete, delete-orphan"
    )
    Sex = Column(String)
