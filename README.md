# metageo_pephub
Automatic Uploader for GEO metadata projects to [PEPhub](https://pephub.databio.org/).

# Overview:
The Metageo_pephub uploader consists of 3 main functionalities:

1) Queuer: A set of functions that searches for new projects in GEO, creates a new cycle for the current run, and records information for each GEO project by setting its status to "queued" and adding it to the database.
2) Uploader: Checks if there are any queued cycles in the Cycle_status table. Gets a list of queued projects, runs Geofetch for them, and uploads the results to Pephubdb using Pepdbagent. Metageo_pephub updates the project uploading status at every step so that it can be checked later to determine why the upload failed and what happened.
3) Checker: Responsible for checking previous cycles, their status, and if they were run. If a cycle was not run or was unsuccessful, it will rerun it. If only one project was unsuccessful, it will try to upload it again.

More information about these processes can be found in the flowcharts and overview below.

![](./docs/img/populator_overview.svg)

## Queuer Flowchart:
```mermaid
%%{init: {'theme':'forest'}}%%
stateDiagram-v2
    s1 --> s2 
    s2 --> s3
    s3 --> s4
    s4 --> s5
    s1: Create a new cycle
    s2: Find GEO updated projects with geofetch Finder
    s3: Add projects to the queue in sample status table
    s4: Change cycle status to queued
    s5: Exit
```

## Uploader Flowchart:

```mermaid
%%{init: {'theme':'forest'}}%%
stateDiagram-v2
    s1 --> s2 
    s2 --> s3
    s3 --> s4
    s4 --> s5
    s5 --> s6
    s6 --> s7
    s7 --> s8

    s7 --> s2
    s6 --> s3

    s1: Get queued cycles by specifying namespace
    s2: Change status of the cycle
    s2: Get each element from list of queued cycle
    s3: Get each project (GSE) from one cycle
    s4: Change status of the project in project_status_table
    s5: Get specified project by running Geofetcher
    s6: Using pepdbagent add project to the DB
    s6: Change status of the project in project_status_table
    s7: Change status of cycle in cycle_status_table
    s8: Exit
```

## Checker Flowchart:
```mermaid
graph TD
    A[Choose cycle to check] --> B{Did it run?}
    B -->|Yes| C{Was it successful?}
    B -->|No| D[Run Queuer for the cycle]
    C -->|Yes| E{Did all samples succeed?}
    C -->|No| D

    D --> D1[Run Uploader for the cycle]
    D1 --> K

    E --> |Yes| K[Exit]
    E --> |No| G[Retrieve failed samples]

    G --> H[Run Queuer for samples]
    H --> F[Run Uploader for queued samples]
    
    F --> I[Change samples status in the table]

    I --> J[Change cycle status in the table]

    J --> K[Exit]

```
