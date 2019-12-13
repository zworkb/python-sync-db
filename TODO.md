# TODOs

## Timeout

when starting a sync and the connection times out,
the update ops get performed on server side but not 
vonfitmed by the client.
a second try for the sync which should be idempotent fails due to duplicate keys.

## Reject on empty push set

when nothing has changed and the push set is empty the push is rejected

