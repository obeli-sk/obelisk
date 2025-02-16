# After launch

## UI
hide OneOff join sets
add request to obtain the stack trace
HAR like view of child executions https://github.com/monasticacademy/httptap

## build
Add -snapshot after release or investigate retaining git ref in --version,
Pass the version to flakes.nix, perhaps use https://crane.dev/API.html e.g. https://github.com/tursodatabase/limbo/blob/main/flake.nix

## CLI
obelisk client component inspect/wit should accept: path, componentId, oci location

## obelisk generate
obelisk generate ext for webhook (just -schedule ext)
obelisk generate config blank -c my.toml
- blank
- testing
- stargazers

obelisk generate workflow ... + host wit files
obelisk add --oci ...

## SAAS
configuration needs to have a name for obelisk deploy.
Investigate warg
