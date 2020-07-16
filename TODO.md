# TODOs

## Timeout

when starting a sync and the connection times out,
the update ops get performed on server side but not 
vonfitmed by the client.
a second try for the sync which should be idempotent fails due to duplicate keys.

## Reject on empty push set

when nothing has changed and the push set is empty the push is rejected

to reproduce it:
    - do some inserts
    - start sync, stop at core.py, line 383 during push() and do hard exit before commit()
    - rerun the sync -> exception
    
explanation:
    - sync was successfully performed on server side
    - but sunc not confirmed on client side

## Remove unnecessary SA objects

- check if the table `sync_content_types` is really used and delete it if it
is superfluous

## Overwork app-test to new infrastructure



## Client side tracking

- server-side tracking and client-side tracking should both use mostly the same library functions

## Server-Side tracking

- hooks for tracking whitelist
- ability to track uploading node also for subsequent server-side changes
    - node_list
    
- pull only fetches operations where whitelist AND node_list matches

## Authentication for sync

   we need a concept for authentication for the websockets communication between client and syncserver