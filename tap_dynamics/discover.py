
from singer.catalog import Catalog, CatalogEntry, Schema
from odata import ODataService
from odata.navproperty import NavigationProperty


def get_schema(entity):
    odata_schema = entity.__odata_schema__
    json_props = {}
    metadata = []
    pks = []
    for odata_prop in odata_schema.get("properties", []):
        odata_type = odata_prop["type"]
        prop_name = odata_prop["name"]
        json_type = "string"
        json_format = None

        inclusion = "available"
        if odata_prop["is_primary_key"] == True:
            pks.append(prop_name)

        metadata.append(
            {
                "breadcrumb": ["properties", prop_name],
                "metadata": {"inclusion": inclusion},
            }
        )

        if odata_type in ["Edm.Date", "Edm.DateTime", "Edm.DateTimeOffset"]:
            json_format = "date-time"
        elif odata_type in ["Edm.Int16", "Edm.Int32", "Edm.Int64"]:
            json_type = "integer"
        elif odata_type in ["Edm.Double", "Edm.Decimal"]:
            json_type = "number"
        elif odata_type == "Edm.Boolean":
            json_type = "boolean"
        
        prop_json_schema = {"type": ["null", json_type]}

        if json_format:
            prop_json_schema["format"] = json_format

        json_props[prop_name] = prop_json_schema

    json_schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": json_props,
    }

    return json_schema, metadata, pks


def get_navigation_properties(entity):
    odata_schema = entity.__odata_schema__
    navigation_properties = []
    for odata_prop in odata_schema.get("navigationProperties", []):
        prop_name = odata_prop["name"]
        navigation_properties.append(prop_name)
    return navigation_properties


def discover(service, get_lookup_tables):
    catalog = Catalog([])
    selected_tables = [
        "accounts",
        "campaigns",
        "leads",
        "savedqueries",
        "userqueries",
        "opportunities",
        "contacts",
        "transactioncurrencies",
        "salesorders",
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


    if get_lookup_tables:
        extra_tables = []
        for entity_name in service.entities.keys():
            if "lkup" in entity_name:
                extra_tables.append(entity_name)
        
        selected_tables += extra_tables
                    
    for entity_name, entity in service.entities.items():
        if entity_name not in selected_tables:
            continue
        schema_dict, metadata, pks = get_schema(entity)
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
    

    if "leads" in service.entities:
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
    
    if "contacts" in service.entities:
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

def clean_view_name(name):
    return name.replace(' ', '-').replace(':', '-')

def create_views_schema(view_name,array_views):

    schema = {
        "stream": view_name,
        "tap_stream_id": view_name,
        "type": ["null", "object"],
        "additionalProperties": True,
        "properties": {}
        
    }

    for item in array_views:
        schema["properties"][clean_view_name(item.name)] = {
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
                    "breadcrumb": ["properties", clean_view_name(item.name)],          
                    "metadata": {"inclusion": "available", "view_id": item.savedqueryid,},
                } 
            )
        elif view_type == 'personal':
            metadata.append(
                {
                    "breadcrumb": ["properties", clean_view_name(item.name)],          
                    "metadata": {"inclusion": "available", "view_id": item.userqueryid,},
                } 
            )

    return metadata

