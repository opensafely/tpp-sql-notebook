from lib.database import patients_with_codes_in_consecutive_quarters
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


def test_patient_with_condition_and_medication():
    session = make_session()
    condition_code = "ASTHMA"
    asthma_medication = MedicationDictionary(
        FullName="Asthma Drug", DMD_ID="0", MultilexDrug_ID="0"
    )
    patient_with_condition = Patient(DateOfBirth="1970/01/01", Sex="M")
    patient_with_condition.CodedEvents.append(CodedEvent(CTV3Code=condition_code))

    patient_with_condition.MedicationIssues = [
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2000-01-01"),
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2000-04-01"),
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2000-07-01"),
    ]
    session.add(patient_with_condition)
    session.commit()

    result = patients_with_codes_in_consecutive_quarters(
        clinical_code_list=[condition_code],
        medicine_code_list=[asthma_medication.DMD_ID],
    )
    assert patient_with_condition.Patient_ID in list(result.Patient_ID)


def test_patient_with_insufficient_medication():
    session = make_session()
    asthma_medication = MedicationDictionary(
        FullName="Asthma Drug", DMD_ID="0", MultilexDrug_ID="0"
    )
    patient_with_condition = Patient(DateOfBirth="1970/01/01", Sex="M")

    patient_with_condition.MedicationIssues = [
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2000-01-01")
    ]
    session.add(patient_with_condition)
    session.commit()

    result = patients_with_codes_in_consecutive_quarters(
        medicine_code_list=[asthma_medication.DMD_ID]
    )
    assert len(result) == 0


def test_patient_with_medication():
    session = make_session()
    asthma_medication = MedicationDictionary(
        FullName="Asthma Drug", DMD_ID="0", MultilexDrug_ID="0"
    )
    patient_with_condition = Patient(DateOfBirth="1970/01/01", Sex="M")

    patient_with_condition.MedicationIssues = [
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2000-01-01"),
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2000-04-01"),
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2000-07-01"),
    ]
    session.add(patient_with_condition)
    session.commit()

    result = patients_with_codes_in_consecutive_quarters(
        medicine_code_list=[asthma_medication.DMD_ID]
    )
    assert patient_with_condition.Patient_ID in list(result.Patient_ID)


def test_two_patients_with_two_medications():
    session = make_session()
    asthma_medication_1 = MedicationDictionary(
        FullName="Asthma Drug 1", DMD_ID="0", MultilexDrug_ID="0"
    )
    asthma_medication_2 = MedicationDictionary(
        FullName="Asthma Drug 2", DMD_ID="1", MultilexDrug_ID="1"
    )

    patient_with_condition_1 = Patient(DateOfBirth="1970/01/01", Sex="M")
    patient_with_condition_2 = Patient(DateOfBirth="1970/01/01", Sex="F")

    patient_with_condition_1.MedicationIssues = [
        MedicationIssue(
            MedicationDictionary=asthma_medication_1, StartDate="2000-01-01"
        ),
        MedicationIssue(
            MedicationDictionary=asthma_medication_1, StartDate="2000-04-01"
        ),
        MedicationIssue(
            MedicationDictionary=asthma_medication_1, StartDate="2000-07-01"
        ),
    ]
    patient_with_condition_2.MedicationIssues = [
        MedicationIssue(
            MedicationDictionary=asthma_medication_2, StartDate="2000-01-01"
        ),
        MedicationIssue(
            MedicationDictionary=asthma_medication_2, StartDate="2000-04-01"
        ),
        MedicationIssue(
            MedicationDictionary=asthma_medication_2, StartDate="2000-07-01"
        ),
    ]
    session.add(patient_with_condition_1)
    session.add(patient_with_condition_2)
    session.commit()

    result = patients_with_codes_in_consecutive_quarters(
        medicine_code_list=[asthma_medication_1.DMD_ID, asthma_medication_2.DMD_ID]
    )
    assert patient_with_condition_1.Patient_ID in list(result.Patient_ID)
    assert patient_with_condition_2.Patient_ID in list(result.Patient_ID)


def test_patient_with_medication():
    session = make_session()
    asthma_medication = MedicationDictionary(
        FullName="Asthma Drug", DMD_ID="0", MultilexDrug_ID="0"
    )
    patient_with_condition = Patient(DateOfBirth="1970/01/01", Sex="M")

    patient_with_condition.MedicationIssues = [
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2000-01-01"),
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2000-04-01"),
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2000-07-01"),
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2001-01-01"),
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2001-04-01"),
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2001-07-01"),
    ]
    session.add(patient_with_condition)
    session.commit()

    result = patients_with_codes_in_consecutive_quarters(
        medicine_code_list=[asthma_medication.DMD_ID]
    )
    assert patient_with_condition.Patient_ID in list(result.Patient_ID)


def test_patient_with_loads_of_medication():
    session = make_session()
    asthma_medication = MedicationDictionary(
        FullName="Asthma Drug", DMD_ID="0", MultilexDrug_ID="0"
    )
    patient_with_condition = Patient(DateOfBirth="1970/01/01", Sex="M")

    patient_with_condition.MedicationIssues = [
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2000-01-01"),
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2000-01-15"),
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2000-03-16"),
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2000-04-01"),
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2000-07-01"),
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2000-08-01"),
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2000-08-01"),
    ]
    session.add(patient_with_condition)
    session.commit()

    result = patients_with_codes_in_consecutive_quarters(
        medicine_code_list=[asthma_medication.DMD_ID]
    )
    assert patient_with_condition.Patient_ID in list(result.Patient_ID)


def test_patient_with_medication_but_gap():
    session = make_session()
    asthma_medication = MedicationDictionary(
        FullName="Asthma Drug", DMD_ID="0", MultilexDrug_ID="0"
    )
    patient_with_condition = Patient(DateOfBirth="1970/01/01", Sex="M")

    patient_with_condition.MedicationIssues = [
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2000-01-01"),
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2000-04-01"),
        MedicationIssue(MedicationDictionary=asthma_medication, StartDate="2000-08-01"),
    ]
    session.add(patient_with_condition)
    session.commit()

    result = patients_with_codes_in_consecutive_quarters(
        medicine_code_list=[asthma_medication.DMD_ID]
    )
    assert len(result.Patient_ID) == 0


def test_patient_with_two_medications():
    session = make_session()
    asthma_medication_1 = MedicationDictionary(
        FullName="Asthma Drug 1", DMD_ID="0", MultilexDrug_ID="0"
    )
    asthma_medication_2 = MedicationDictionary(
        FullName="Asthma Drug 2", DMD_ID="1", MultilexDrug_ID="1"
    )
    patient_with_condition = Patient(DateOfBirth="1970/01/01", Sex="M")

    patient_with_condition.MedicationIssues = [
        MedicationIssue(
            MedicationDictionary=asthma_medication_1, StartDate="2000-01-01"
        ),
        MedicationIssue(
            MedicationDictionary=asthma_medication_2, StartDate="2000-04-01"
        ),
        MedicationIssue(
            MedicationDictionary=asthma_medication_1, StartDate="2000-07-01"
        ),
    ]
    session.add(patient_with_condition)
    session.commit()

    result = patients_with_codes_in_consecutive_quarters(
        medicine_code_list=[asthma_medication_1.DMD_ID, asthma_medication_2.DMD_ID]
    )
    assert patient_with_condition.Patient_ID in list(result.Patient_ID)


def test_patient_with_two_medications_and_strict_mode():
    session = make_session()
    asthma_medication_1 = MedicationDictionary(
        FullName="Asthma Drug 1", DMD_ID="0", MultilexDrug_ID="0"
    )
    asthma_medication_2 = MedicationDictionary(
        FullName="Asthma Drug 2", DMD_ID="1", MultilexDrug_ID="1"
    )
    patient_with_condition = Patient(DateOfBirth="1970/01/01", Sex="M")

    patient_with_condition.MedicationIssues = [
        MedicationIssue(
            MedicationDictionary=asthma_medication_1, StartDate="2000-01-01"
        ),
        MedicationIssue(
            MedicationDictionary=asthma_medication_2, StartDate="2000-04-01"
        ),
        MedicationIssue(
            MedicationDictionary=asthma_medication_1, StartDate="2000-07-01"
        ),
    ]
    session.add(patient_with_condition)
    session.commit()

    result = patients_with_codes_in_consecutive_quarters(
        medicine_code_list=[asthma_medication_1.DMD_ID, asthma_medication_2.DMD_ID],
        strict_codes=True,
    )
    assert len(result) == 0
