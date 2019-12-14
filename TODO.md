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