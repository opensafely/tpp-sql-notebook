from lib.database import patients_with_age_condition
from tests.database import make_database, make_session
from tests.database import CodedEvent, MedicationIssue, MedicationDictionary, Patient


def setup_module(module):
    make_database()


def setup_function(function):
    """Ensure test database is empty
    """
    session = make_session()
    session.query(CodedEvent).delete()
    session.query(MedicationIssue).delete()
    session.query(MedicationDictionary).delete()
    session.query(Patient).delete()
    session.commit()


def test_age():
    session = make_session()
    old_patient = Patient(DateOfBirth="1900/01/01")
    young_patient_1 = Patient(DateOfBirth="2000/01/01")
    young_patient_2 = Patient(DateOfBirth="2001/01/01")
    session.add(old_patient)
    session.add(young_patient_1)
    session.add(young_patient_2)
    session.commit()

    result = patients_with_age_condition(">", 70)
    assert old_patient.Patient_ID in list(result.Patient_ID)

    result = patients_with_age_condition("<", 70)
    assert young_patient_1.Patient_ID in list(result.Patient_ID)
    assert young_patient_2.Patient_ID in list(result.Patient_ID)
