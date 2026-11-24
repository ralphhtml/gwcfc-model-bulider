# gwcfc-model-builder

Draws weather model runs for the Guta Forecasting Center radar site (https://radar.gwcfc.net/) on GitHub's
machines and uploads them to the site's parsing server.

## Why this exists

The parsing server is small: one HRRR forecast hour takes it about 25 minutes, so it could never finish a run before
the next one arrived. The same hour takes about 30 seconds here. So the runs are drawn here on a schedule and sent
to the server, which only stores them and hands them out.

## How it works

- `.github/workflows/build.yml` runs on a schedule and calls `build.py`.
- `build.py` finds the newest complete run, asks the server whether it already has it, draws it, and uploads it in signed parts.
- `pipeline.bin` is the parsing server's own drawing code, encrypted (AES-256). It is not readable from this
  repository: a build unlocks it with a key derived from a repository secret.

## Security

- Two repository secrets are needed: `GWCFC_BASE` (the server's address) and `GWCFC_UPLOAD_KEY` (a private signing
  key). Neither is ever printed or written to a log, and neither belongs in this repository.
- Every upload is signed. The server holds only the matching public key and refuses anything that is not signed,
  recent and complete. Nobody without the private key can upload.
- The workflow never runs on pull requests, so code from a fork never sees the secrets.

## Run it by hand

Actions > Build models > Run workflow. Leave the boxes empty for the newest run, or fill in a run such as
`20261123_18`.
