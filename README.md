# tap-dynamics-crm

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Dynamics 365 CRM OData API](https://docs.microsoft.com/en-us/dynamics365/customerengagement/on-premises/developer/use-microsoft-dynamics-365-web-api)
- Discovers all available resources and outputs the schema for each resource

## Usage

To run `tap-dynamics` with the configuration file, use this command:

```sh
tap-dynamics -c config.json
```

## Example config

```json
{
  "start_date": "2017-09-10",
  "domain": "dreamdata.crm4",
  "client_id": "<client_id>",
  "client_secret": "<client_secret>",
  "redirect_uri": "<redirect_url>",
  "refresh_token": "<refresh_token>",
  "get_all_tables": false,
  "get_saved_queries": false,
  "get_lookup_tables": false
}
```

### Configuration Options

- `start_date`: The start date for data extraction (required)
- `domain`: Your Dynamics 365 CRM domain (required)
- `client_id`: OAuth client ID (required)
- `client_secret`: OAuth client secret (required)
- `redirect_uri`: OAuth redirect URI (required)
- `refresh_token`: OAuth refresh token (required)
- `get_all_tables`: When `true`, discovers and includes all available tables in the catalog. When `false` (default), only includes a predefined set of selected tables.
- `get_saved_queries`: When `true`(default), includes saved queries and user queries for leads and contacts as additional streams. When `false`, these query streams are not included.
- `get_lookup_tables`: When `true`, includes lookup tables (tables with "lkup" in their name) in addition to the predefined selected tables. This is not necessary when `get_all_tables` is `true` since all tables will already be included.
