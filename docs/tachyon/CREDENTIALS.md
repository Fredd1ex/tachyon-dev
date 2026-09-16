# Credentials

API keys are never saved in Tachyon configuration or plaintext fallback files.
`tachyon providers login` reads the key without echoing it or putting it in
process arguments. It does not contact OpenRouter to validate the key.

## Linux Storage

Login first writes to an unlocked persistent default Secret Service collection
on the user session D-Bus. Tachyon does not install a provider, create a
collection, or request that a locked collection be unlocked. A default alias
pointing to the session collection is rejected as nonpersistent.

If the persistent write fails, login tries the Linux kernel keyring using the
same service/account as the previous kernel-only backend. Success returns a
typed `StoreStatus::Persistent` or `StoreStatus::Volatile`; the CLI explicitly
warns that volatile storage is lost on reboot (or earlier if cleared). If both
writes fail, login reports failure without including backend payloads or keys.
Kernel keyring support and access permissions are required for fallback; some
containers and restricted sessions deny access.

## Lookup Order

1. `OPENROUTER_API_KEY`, when present, bypasses all credential stores. Existing
   empty/placeholder filtering is unchanged; such an environment value does not
   cause a store lookup.
2. An existing volatile kernel key overrides persistent storage, even after
   Secret Service becomes available or unlocked. This ensures that a newer
   fallback login does not silently switch back to an older persistent key.
3. If no volatile key exists, read persistent storage. A missing persistent key
   is absent; an unavailable/locked persistent store is an actionable error.

If the kernel store cannot be read, lookup fails rather than risk using a stale
persistent key. An environment override still works. A persistent login clears
the volatile override without writing a second copy there. If that cleanup
fails, login reports partial failure: the persistent key was saved, but an old
volatile key may still take precedence. Retry when both stores are accessible.

**Reboot limitation:** a fallback login cannot update an inaccessible persistent
key. Once the volatile key disappears, an older persistent key may become active
again. Unlock/configure Secret Service and log in again with the desired key to
make the change persistent. There is no automatic migration or durable marker.
Concurrent logins/logout are not a cross-store transaction; serialize them.

## Logout

`tachyon providers logout` attempts removal from both Linux stores, even if one
fails. Already absent entries count as success. Failures identify each store
whose removal failed and warn that credentials may remain; retry when it is
accessible. Logout does not unset `OPENROUTER_API_KEY`. Remove that value
separately from the environment used to launch the daemon.

The daemon may retain a previously loaded key. Restart it after login/logout to
reload or clear credentials; these commands only print restart guidance and do
not perform the restart. macOS and Windows retain their native credential-store
backends, without the Linux fallback.

## Tests

Default credential and login tests inject in-memory fakes; they never access
real API keys or credential stores. The ignored persistent-store round-trip
test is opt-in only, uses a unique test service and a fake credential, and must
only be run against an explicitly configured disposable store.
