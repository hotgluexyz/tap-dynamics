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



def transform_entity_to_json_schema(entity_name, entity, service):
    if entity_name not in ["accounts", "campaigns", "leads", "contacts", "opportunities", "salesorders"]:
        return get_schema(entity)

    json_schema = {}
    odata_schema = entity.__odata_schema__
    metadata = []
    pks = []
    json_props = {}
    try:
        first_entity = list(service.query(entity))[0]
    except:
        return get_schema(entity)

    for attr in dir(first_entity):
        # Check type for attribute, so we can map child lookup and choices
        if "__" in attr:
            continue
        
        this_odata_prop = {}
        for odata_prop in odata_schema.get("properties", []):
            if odata_prop["name"] == attr:
                this_odata_prop = odata_prop
                break

        if this_odata_prop.get("is_primary_key") == True:
            pks.append(attr)
        
        odata_type = this_odata_prop.get("type")
        
        metadata.append(
            {
                "breadcrumb": ["properties", attr],
                "metadata": {"inclusion": "available"},
            }
        )

        attr_type = type(getattr(first_entity, attr)).__name__
        
        if odata_type is not None:
            if odata_type in ["Edm.Date", "Edm.DateTime", "Edm.DateTimeOffset"]:
                json_type = "date-time"
            elif odata_type in ["Edm.Int16", "Edm.Int32", "Edm.Int64"]:
                json_type = "integer"
            elif odata_type in ["Edm.Double", "Edm.Decimal"]:
                json_type = "number"
            elif odata_type == "Edm.Boolean":
                json_type = "boolean"
            elif odata_type == "Edm.Guid":
                json_type = "object"
        else:
            if attr_type in ["str", "int", "float", "bool"]:
                json_type = attr_type
            elif attr_type == "list":
                json_type = "array"
            elif attr_type == "dict":
                json_type = "object"
            elif attr_type == "datetime":
                json_type = "datetime"
            else:
                json_type = "object"
    
        json_props[attr] = {"type": [json_type, "null"]}

    json_schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": json_props,
    }

    return json_schema, metadata, pks

