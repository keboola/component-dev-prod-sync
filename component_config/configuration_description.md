Transfers all configurations and orchestrations from the source (PROD) to the destination (DEV) project and vice versa.

Updates or creates new row/configuration.

Secret values are not transferred if present in remote project.

**NOTE** Current version does not take configuration deletes into account.

**NOTE2** Ochestration IDs cannot be mapped 1:1 across projects. 
Internal mapping of transferred orchestration IDs is stored in the state file. If any of the orchestrations is removed from the destination, 
no changes will be transferred.