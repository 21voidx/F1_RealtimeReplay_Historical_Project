"""Legacy cloud consumer placeholder.

The localhost version uses Native SingleStore Pipelines defined in
`singlestore/init.sql`, so this manual Python SingleStore consumer is not
started by Docker Compose. The original cloud script has been disabled and
redacted under `_legacy_cloud_disabled/` to avoid shipping credentials.
"""

if __name__ == "__main__":
    print("Use Native SingleStore Pipelines in singlestore/init.sql for the localhost project.")
