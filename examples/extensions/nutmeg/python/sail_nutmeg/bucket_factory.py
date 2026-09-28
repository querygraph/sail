"""The `nutmeg_bucket(key, n)` function: the engine's own hash bucket of a key.

A functions-only extension placed `any`, because the host refuses scalar
functions from a driver-only extension. Nutmeg.checkpoint(mode="distributed")
writes `partitionBy` on this bucket through Sail's ordinary writer and the
`checkpointed` scan declares the layout truthfully.
"""


class Extension:
    def manifest(self):
        return {
            "name": "nutmeg-bucket",
            "version": "0.1.0",
            "api_version": 1,
            "datafusion_version": "55.1.0",
            "arrow_version": "59.3.0",
            "placement": "any",
            "relation_types": [],
        }

    def bind(self, session_id):
        from ._native import BoundBucket
        return BoundBucket(session_id)


def extension():
    return Extension()
