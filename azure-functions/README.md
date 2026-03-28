# whoami API (Azure Functions)

This directory houses code which can be deployed to an Azure Functions Application which is responsible for defining Python-based workflows related to data ingestion.

Once deployed, the following Functions are created inside of Azure:
- `github_events_import` - queries the GitHub Events API for a configured User, and persists the results in SQL Server.