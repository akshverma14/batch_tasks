from ..models.db_models import *
from config.cms_config import *


def match_salesforce_contact_ids(job_id: str, session):
    """
    Function to match salesforce type contacts in Contact Identifier Table.
    If match found:
        Status of contact in Contact Staging Table is updated to "MATCHED"
    """
    try:
        matched_contact_list = list()
        for _, identifier_group, identifier in get_salesforce_matching_fields(
        ):
            matched_contacts = session.query(
                ContactStagingTable, ContactIdentifierTable
            ).filter(ContactStagingTable.jobid__c == job_id).filter(
                and_(
                    and_(
                        getattr(ContactIdentifierTable,
                                IdentifierColumns.identifier.value) == getattr(
                                    ContactStagingTable, identifier),
                        getattr(ContactIdentifierTable,
                                IdentifierColumns.identifier_group.value) ==
                        identifier_group),
                    func.lower(
                        getattr(
                            ContactIdentifierTable,
                            IdentifierColumns.matm_owner.value)) == func.lower(
                                getattr(
                                    ContactStagingTable,
                                    StagingColumns.source_name.value)))).all()

            matched_contact_list = matched_contact_list + matched_contacts
        for staged_contact, identified_contact in matched_contact_list:
            staged_contact.contactid__c = identified_contact.contact_id__c
            staged_contact.status__c = RecordStatus.matched.value
        session.flush()
        session.commit()
    except Exception as e:
        session.rollback()
        log(f"{e}")


def update_client_type(session):
    """
    Function to Update new records in Contact Staging Table with source type and source name
    from Organisation Source Table.
ss
    """
    try:
        job_id = uuid4().hex
        unmatched_data = session.query(
            ContactStagingTable, OrganizationSourceTable).filter(
                func.lower(
                    getattr(ContactStagingTable, StagingColumns.status.value))
                == RecordStatus.inserted.value.lower(),
                ContactStagingTable.organisationsourceid__c ==
                OrganizationSourceTable.client_id,
            ).limit(100).all()

        for staged_data, org_data in unmatched_data:
            staged_data.source_type__c = org_data.client_type
            staged_data.source_name__c = org_data.source_name
            staged_data.status__c = RecordStatus.unmatched.value
            staged_data.jobid__c = job_id
        session.flush()
        session.commit()
        return job_id
    except Exception as e:
        session.rollback()
        log(f"{e}")


def match_contact_ids():
    """
    Function to initiate
    """
    try:
        session = SessionLocal()
        job_id = update_client_type(session)
        match_salesforce_contact_ids(job_id, session)
        session.close()
    except Exception as e:
        log(f"{e}")


if __name__ == '__main__':
    match_contact_ids()
