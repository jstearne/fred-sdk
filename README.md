# fred-sdk
FRED API example SDK connector

.gitignore for configuration.json, containing the FRED API Key (it's free). Simply add the API key in the following format in a file named configuration.json:

```
{
    "fred_api_key": "<YOUR API KEY HERE>"
}
```

Builds a table of category data (specifically category 125). Can easily be modified to pull all data, additional tables, incremental syncs