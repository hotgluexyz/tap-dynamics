
from singer.catalog import Catalog, CatalogEntry, Schema
from odata import ODataService
from odata.navproperty import NavigationProperty
from utils import transform_entity_to_json_schema, get_schema


def discover(service, get_lookup_tables):
    default_formatted_value_name = "OData.Community.Display.V1.FormattedValue"
    catalog = Catalog([])
    selected_tables = [
        "accounts",
        "campaigns",
        "leads",
        "contacts",
        "opportunities",
        "salesorders",
        "transactioncurrencies",
        "savedqueries",
        "userqueries",
        "systemusers",
        "msdyncrm_linkedinaccounts",
        "msdyncrm_linkedinactivities",
        "msdyncrm_linkedincampaigns",
        "msdyncrm_linkedinconfigurations",
        "msdyncrm_linkedinfieldmappings",
        "msdyncrm_linkedinformanswers",
        "msdyncrm_linkedinformquestions",
        "msdyncrm_linkedinforms",
        "msdyncrm_linkedinformsubmissions",
        "msdyncrm_linkedinleadmatchingstrategies",
        "msdyncrm_linkedinuserprofile_accountset",
        "msdyncrm_linkedinuserprofiles",
        "msdyncrm_msdyncrm_linkedinlms_fieldmappingset",
    ]


    # if get_lookup_tables:
    #     extra_tables = []
    #     for entity_name in service.entities.keys():
    #         if "lkup" in entity_name:
    #             extra_tables.append(entity_name)
        
    #     selected_tables += extra_tables
                    
    for entity_name, entity in service.entities.items():
        if entity_name not in selected_tables:
            continue
        schema_dict, metadata, pks = get_schema(entity)
        # get table data to get lookup values
        resp = service.default_context.connection._do_get(f"{service.url}{entity_name}").json()
        # get the first result and adds and modifies fields with
        # lookup values
        try:
            first = resp["value"][0]
        except IndexError:
            first = {}
        

        possible_choice_fields = []
        for key in first.keys():
            if default_formatted_value_name in key and key not in ["createdon", "modifiedon"]:
                possible_choice_fields.append(key.replace(f"@{default_formatted_value_name}", ""))

        for choice in possible_choice_fields:
            schema_dict["properties"][choice] = {"type": ["null", "string"]}

        metadata.append({"breadcrumb": [], "metadata": {"selected": True}})
        schema = Schema.from_dict(schema_dict)
        catalog.streams.append(
            CatalogEntry(
                stream=entity_name,
                tap_stream_id=entity_name,
                key_properties=pks,
                schema=schema,
                metadata=metadata,
                replication_method="INCREMENTAL"
                if schema_dict.get("properties", None).get("createdon", None)
                else "FULL_TABLE",
            )
        )
    
    view_leads_data = get_view_by_service('savedqueries','lead',service) 
    if len(view_leads_data) > 0 : 
        catalog.streams.append(
                CatalogEntry(
                    stream="view_leads",
                    tap_stream_id="view_leads",
                    key_properties=None,
                    schema=create_views_schema('view_leads',view_leads_data),
                    metadata=create_metadata_views('system',view_leads_data),
                )
            )
    view_personal_leads_data = get_view_by_service('userqueries','lead',service) 
    if len(view_personal_leads_data) > 0 : 
        catalog.streams.append(
                CatalogEntry(
                    stream="view_personal_leads",
                    tap_stream_id="view_personal_leads",
                    key_properties=None,
                    schema=create_views_schema('view_personal_leads',view_personal_leads_data),
                    metadata=create_metadata_views('personal',view_personal_leads_data),
                )
            )
    view_contacts_data = get_view_by_service('savedqueries','contact',service)
    if len(view_contacts_data) > 0 : 
        catalog.streams.append(
                CatalogEntry(
                    stream="view_contacts",
                    tap_stream_id="view_contacts",
                    key_properties=None,
                    schema=create_views_schema('view_contacts',view_contacts_data),
                    metadata=create_metadata_views('system',view_contacts_data),
                )
            )
    view_personal_contacts_data = get_view_by_service('userqueries','contact',service) 
    if len(view_personal_contacts_data) > 0:
        catalog.streams.append(
                CatalogEntry(
                    stream="view_personal_contacts",
                    tap_stream_id="view_personal_contacts",
                    key_properties=None,
                    schema=create_views_schema('view_personal_contacts',view_personal_contacts_data),
                    metadata=create_metadata_views('personal',view_personal_contacts_data),
                )
            )
    
    return catalog


def get_view_by_service(entity, name, service):
 
    view = service.entities[entity]
    query = service.query(view)
    query = query.filter(f"returnedtypecode eq '{name}'")
    array_items = []
    for item in query:
        array_items.append(item)

    return array_items


def create_views_schema(view_name,array_views):

    schema = {
        "stream": view_name,
        "tap_stream_id": view_name,
        "type": ["null", "object"],
        "additionalProperties": True,
        "properties": {}
        
    }

    for item in array_views:
        schema["properties"][item.name] = {
          "type": ["null", "string"]
        }
    
    return Schema.from_dict(schema)

def create_metadata_views(view_type,array_views):
    
    metadata = [{
          "breadcrumb": [],
          "metadata": {
            "selected": True
          }}]

    for item in array_views:

        if view_type == 'system':
            metadata.append(
                {
                    "breadcrumb": ["properties", item.name],          
                    "metadata": {"inclusion": "available", "view_id": item.savedqueryid,},
                } 
            )
        elif view_type == 'personal':
            metadata.append(
                {
                    "breadcrumb": ["properties", item.name],          
                    "metadata": {"inclusion": "available", "view_id": item.userqueryid,},
                } 
            )

    return metadata

