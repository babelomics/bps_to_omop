"""
Module providing pyarrow scheme options for an OMOP-CDM instance

The structure is (field_name, field_datatype, is_nullable)
"""

from pyarrow import date32, float64, int64, schema, string, timestamp

omop_schemas = {
    "CDM_SOURCE": schema(
        [
            ("cdm_source_name", string(), False),
            ("cdm_source_abbreviation", string(), False),
            ("cdm_holder", string(), False),
            ("source_description", string(), True),
            ("source_documentation_reference", string(), True),
            ("cdm_etl_reference", string(), True),
            ("source_release_date", date32(), False),
            ("cdm_release_date", date32(), False),
            ("cdm_version", string(), True),
            ("cdm_version_concept_id", int64(), False),
            ("vocabulary_version", string(), False),
        ]
    ),
    "PERSON": schema(
        [
            ("person_id", int64(), False),
            ("gender_concept_id", int64(), False),
            ("year_of_birth", int64(), False),
            ("month_of_birth", int64(), True),
            ("day_of_birth", int64(), True),
            ("birth_datetime", timestamp("us"), True),
            ("race_concept_id", int64(), False),
            ("ethnicity_concept_id", int64(), False),
            ("location_id", int64(), True),
            ("provider_id", int64(), True),
            ("care_site_id", int64(), True),
            ("person_source_value", string(), True),
            ("gender_source_value", string(), True),
            ("gender_source_concept_id", int64(), True),
            ("race_source_value", string(), True),
            ("race_source_concept_id", int64(), True),
            ("ethnicity_source_value", string(), True),
            ("ethnicity_source_concept_id", int64(), True),
        ]
    ),
    "OBSERVATION_PERIOD": schema(
        [
            ("observation_period_id", int64(), False),
            ("person_id", int64(), False),
            ("observation_period_start_date", date32(), False),
            ("observation_period_end_date", date32(), False),
            ("period_type_concept_id", int64(), False),
        ]
    ),
    "VISIT_OCCURRENCE": schema(
        [
            ("visit_occurrence_id", int64(), False),
            ("person_id", int64(), False),
            ("visit_concept_id", int64(), False),
            ("visit_start_date", date32(), False),
            ("visit_start_datetime", timestamp("us"), True),
            ("visit_end_date", date32(), False),
            ("visit_end_datetime", timestamp("us"), True),
            ("visit_type_concept_id", int64(), False),
            ("provider_id", int64(), True),
            ("care_site_id", int64(), True),
            ("visit_source_value", string(), True),
            ("visit_source_concept_id", int64(), True),
            ("admitted_from_concept_id", int64(), True),
            ("admitted_from_source_value", string(), True),
            ("discharged_to_concept_id", int64(), True),
            ("discharged_to_source_value", string(), True),
            ("preceding_visit_occurrence_id", int64(), True),
        ]
    ),
    "CONDITION_OCCURRENCE": schema(
        [
            ("condition_occurrence_id", int64(), False),
            ("person_id", int64(), False),
            ("condition_concept_id", int64(), False),
            ("condition_start_date", date32(), False),
            ("condition_start_datetime", timestamp("us"), True),
            ("condition_end_date", date32(), True),
            ("condition_end_datetime", timestamp("us"), True),
            ("condition_type_concept_id", int64(), False),
            ("condition_status_concept_id", int64(), True),
            ("stop_reason", string(), True),
            ("provider_id", int64(), True),
            ("visit_occurrence_id", int64(), True),
            ("visit_detail_id", int64(), True),
            ("condition_source_value", string(), True),
            ("condition_source_concept_id", int64(), True),
            ("condition_status_source_value", string(), True),
        ]
    ),
    "CONCEPT": schema(
        [
            ("concept_id", int64(), False),
            ("concept_name", string(), False),
            ("domain_id", string(), False),
            ("vocabulary_id", string(), False),
            ("concept_class_id", string(), False),
            ("standard_concept", string(), True),
            ("concept_code", string(), False),
            ("valid_start_date", date32(), False),
            ("valid_end_date", date32(), False),
            ("invalid_reason", string(), True),
        ]
    ),
    "CONCEPT_RELATIONSHIP": schema(
        [
            ("concept_id_1", int64(), False),
            ("concept_id_2", int64(), False),
            ("relationship_id", string(), False),
            ("valid_start_date", date32(), False),
            ("valid_end_date", date32(), False),
            ("invalid_reason", string(), True),
        ]
    ),
    "SOURCE_TO_CONCEPT_MAP": schema(
        [
            ("source_code", string(), False),
            ("source_concept_id", int64(), False),
            ("source_vocabulary_id", string(), False),
            ("source_code_description", string(), True),
            ("target_concept_id", int64(), False),
            ("target_vocabulary_id", string(), True),
            ("valid_start_date", date32(), False),
            ("valid_end_date", date32(), False),
            ("invalid_reason", string(), True),
        ]
    ),
    "VOCABULARY": schema(
        [
            ("vocabulary_id", string(), False),
            ("vocabulary_name", string(), False),
            ("vocabulary_reference", string(), True),
            ("vocabulary_version", string(), True),
            ("vocabulary_concept_id", int64(), False),
        ]
    ),
    "MEASUREMENT": schema(
        [
            ("measurement_id", int64(), False),
            ("person_id", int64(), False),
            ("measurement_concept_id", int64(), False),
            ("measurement_date", date32(), False),
            ("measurement_datetime", timestamp("us"), True),
            ("measurement_time", string(), True),
            ("measurement_type_concept_id", int64(), False),
            ("operator_concept_id", int64(), True),
            ("value_as_number", float64(), True),
            ("value_as_concept_id", int64(), True),
            ("unit_concept_id", int64(), True),
            ("range_low", float64(), True),
            ("range_high", float64(), True),
            ("provider_id", int64(), True),
            ("visit_occurrence_id", int64(), True),
            ("visit_detail_id", int64(), True),
            ("measurement_source_value", string(), True),
            ("measurement_source_concept_id", int64(), True),
            ("unit_source_value", string(), True),
            ("unit_source_concept_id", int64(), True),
            ("value_source_value", string(), True),
            ("measurement_event_id", int64(), True),
            ("meas_event_field_concept_id", int64(), True),
        ]
    ),
    "PROVIDER": schema(
        [
            ("provider_id", int64(), False),
            ("provider_name", string(), True),
            ("npi", string(), True),
            ("dea", string(), True),
            ("specialty_concept_id", int64(), True),
            ("care_site_id", int64(), True),
            ("year_of_birth", int64(), True),
            ("gender_concept_id", int64(), True),
            ("provider_source_value", string(), True),
            ("specialty_source_value", string(), True),
            ("specialty_source_concept_id", int64(), True),
            ("gender_source_value", string(), True),
            ("gender_source_concept_id", int64(), True),
        ]
    ),
    "DEATH": schema(
        [
            ("person_id", int64(), False),
            ("death_date", date32(), False),
            ("death_datetime", timestamp("us"), True),
            ("death_type_concept_id", int64(), True),
            ("cause_concept_id", int64(), True),
            ("cause_source_value", string(), True),
            ("cause_source_concept_id", int64(), True),
        ]
    ),
    "COHORT": schema(
        [
            ("cohort_definition_id", int64(), False),
            ("subject_id", int64(), False),
            ("cohort_start_date", date32(), False),
            ("cohort_end_date", date32(), False),
        ]
    ),
    "COHORT_DEFINITION": schema(
        [
            ("cohort_definition_id", int64(), False),
            ("cohort_definition_name", string(), False),
            ("cohort_definition_description", string(), True),
            ("definition_type_concept_id", int64(), False),
            ("cohort_definition_syntax", string(), True),
            ("subject_concept_id", int64(), False),
            ("cohort_initiation_date", date32(), True),
        ]
    ),
    "LOCATION": schema(
        [
            ("location_id", int64(), False),
            ("address_1", string(), False),
            ("address_2", string(), False),
            ("city", string(), True),
            ("state", string(), True),
            ("zip", string(), True),
            ("county", string(), False),
            ("location_source_value", string(), False),
            ("country_concept_id", int64(), True),
            ("country_source_value", string(), True),
            ("latitude", float64(), True),
            ("longitude", float64(), True),
        ]
    ),
}
