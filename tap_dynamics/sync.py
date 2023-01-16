from datetime import datetime

import singer
from singer import metrics, metadata, Transformer
from singer.bookmarks import set_currently_syncing

from tap_dynamics.discover import discover

LOGGER = singer.get_logger()

MODIFIED_DATE_FIELD = "modifiedon"


def get_bookmark(state, stream_name, default):
    return state.get("bookmarks", {}).get(stream_name, default)


def write_bookmark(state, stream_name, value):
    if "bookmarks" not in state:
        state["bookmarks"] = {}
    state["bookmarks"][stream_name] = value
    singer.write_state(state)


def write_schema(stream):
    schema = stream.schema.to_dict()
    singer.write_schema(stream.tap_stream_id, schema, stream.key_properties)


def sync_stream(service, catalog, state, start_date, stream, mdata):
    stream_name = stream.tap_stream_id
    last_datetime = get_bookmark(state, stream_name, start_date)

    write_schema(stream)

    max_modified = last_datetime

    ## TODO: add metrics?
    entitycls = service.entities[stream_name]
    query = service.query(entitycls)

    if hasattr(entitycls, MODIFIED_DATE_FIELD):
        LOGGER.info(
            "{} - Syncing data since {}".format(stream.tap_stream_id, last_datetime)
        )
        query = query.filter(
            getattr(entitycls, MODIFIED_DATE_FIELD)
            >= singer.utils.strptime_with_tz(last_datetime)
        ).order_by(getattr(entitycls, MODIFIED_DATE_FIELD).asc())
    else:
        LOGGER.info("{} - Syncing using full replication".format(stream.tap_stream_id))

    schema = stream.schema.to_dict()

    count = 0
    with metrics.http_request_timer(stream.tap_stream_id):
        with metrics.record_counter(stream.tap_stream_id) as counter:
            for record in query:
                dict_record = {}
                for odata_prop in entitycls.__odata_schema__["properties"]:
                    prop_name = odata_prop["name"]
                    value = getattr(record, prop_name)
                    if isinstance(value, datetime):
                        value = singer.utils.strftime(value)
                    dict_record[prop_name] = value

                if MODIFIED_DATE_FIELD in dict_record:
                    if dict_record[MODIFIED_DATE_FIELD] > max_modified:
                        max_modified = dict_record[MODIFIED_DATE_FIELD]
                    else:
                        continue

                with Transformer() as transformer:
                    dict_record = transformer.transform(dict_record, schema, mdata)
                singer.write_record(stream.tap_stream_id, dict_record)
                counter.increment()

                count += 1
                if count % 5000 == 0:
                    write_bookmark(state, stream_name, max_modified)

    write_bookmark(state, stream_name, max_modified)


def update_current_stream(state, stream_name=None):
    set_currently_syncing(state, stream_name)
    singer.write_state(state)


def sync(service, catalog, state, start_date):
    if not catalog:
        catalog = discover(service)
        selected_streams = catalog.streams
    else:
        selected_streams = catalog.get_selected_streams(state)
        
    for stream in selected_streams:
        if stream.tap_stream_id == "view_leads":
            stream.views = get_views_by_metadata(stream.metadata)
            for stream_catalog in catalog.streams:
                if stream_catalog.tap_stream_id == "leads":
                    stream.metadata = stream_catalog.metadata
                    stream.key_properties = stream_catalog.key_properties
            mdata = metadata.to_map(stream.metadata)
           
            update_current_stream(state, stream.tap_stream_id)
            sync_stream_views(service, stream)

                        
        else:  
            mdata = metadata.to_map(stream.metadata)
            update_current_stream(state, stream.tap_stream_id)
            sync_stream(service, catalog, state, start_date, stream, mdata)

    update_current_stream(state)


def get_views_by_metadata(metadata):
    selected_views = {}
    for metadata_entry in metadata:
        if metadata_entry.get('metadata').get('selected') == True:
            if len(metadata_entry.get('breadcrumb')) > 0:
                view_name = metadata_entry.get('breadcrumb')[1]
                view_id = metadata_entry.get('metadata').get('view_id')
                selected_views[view_name] = view_id
                
    return selected_views

def get_leads_by_view(service,dict_view_leads):
    entitycls = service.entities['leads']
    query = service.query(entitycls)
    leads = {}
    for view_name,view_id in dict_view_leads.items():
        try:
            lead = query.raw({'savedQuery': "{}".format(view_id)})
            leads[view_name]=lead
        except:
            LOGGER.info("View not found: %s", view_id)
        
    return leads


def sync_stream_views(service, stream):

   
      
    leads = get_leads_by_view(service,stream.views)

    for view_lead, leads in leads.items():
        custom_schema = create_schema_properties(leads)
        singer.write_schema(view_lead, custom_schema, stream.key_properties)
        if len(leads) > 0:
            for record in leads:
                if record.get("@odata.etag"):
                    record.pop("@odata.etag")
                singer.write_record(view_lead, record)

def create_schema_properties(leads):
    
    schema ={
        "properties" : {},
        "type" :"object",
        "additionalProperties": True
    }
    
    if len(leads) > 0:
        for fields in leads:
            fields.pop("@odata.etag")
            for field in fields.keys():
               
                schema['properties'][field] = {
                    'type' : ['integer', 'number', 'string', 'null']
                }
    return schema