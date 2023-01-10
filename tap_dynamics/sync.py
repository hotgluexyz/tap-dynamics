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
            array_of_view_leads = get_views_by_metadata(stream.metadata)
            
            for stream_catalog in catalog.streams:
                if stream_catalog.tap_stream_id == "leads":
                    stream.metadata = stream_catalog.metadata
                    stream.schema = stream_catalog.schema
                    stream.key_properties = stream_catalog.key_properties
            mdata = metadata.to_map(stream.metadata)
            stream.views = array_of_view_leads
            update_current_stream(state, stream.tap_stream_id)
            sync_stream_views(service, catalog, state, start_date, stream, mdata)

                
           
        else:  
            mdata = metadata.to_map(stream.metadata)
            update_current_stream(state, stream.tap_stream_id)
            sync_stream(service, catalog, state, start_date, stream, mdata)

    update_current_stream(state)


def get_views_by_metadata(metadata):
    selected_views = []
    for metadata_entry in metadata:
        if metadata_entry.get('metadata').get('selected') == True:
            if len(metadata_entry.get('breadcrumb')) > 0:
                view_name = metadata_entry.get('breadcrumb')[1]
                view_id = view_name.split('_id:')[1]
                selected_views.append(view_id)
    return selected_views

def get_leads_by_view(query,array_view_leads):
    
    leads = []
    for view_id in array_view_leads:
        try:
            lead = query.raw({'savedQuery': "{}".format(view_id)})
            leads.append(lead)
        except:
            LOGGER.info("View not found: %s", view_id)
        
    return leads


def sync_stream_views(service, catalog, state, start_date, stream, mdata):
    stream_name = stream.tap_stream_id
    last_datetime = get_bookmark(state, stream_name, start_date)

    write_schema(stream)

    max_modified = last_datetime

    entitycls = service.entities['leads']
    query = service.query(entitycls)
    leads = get_leads_by_view(query,stream.views)
    for arr_lead in leads:
        if len(arr_lead) > 0:
            for record in arr_lead:
                singer.write_record(stream.tap_stream_id, record)

