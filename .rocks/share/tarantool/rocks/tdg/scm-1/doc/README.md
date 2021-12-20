# Tarantool Data Grid documentation

Published at https://www.tarantool.io/en/tdg/latest

## Building documentation using [Docker](https://www.docker.com)

### Pull the doc-builder Docker image

```bash
docker pull tarantool/doc-builder
```

### Build TDG documentation using the Docker image

Run a required make command inside a ``tarantool/doc-builder`` container:
```bash
docker run --rm -it -v $(pwd):/doc tarantool/doc-builder sh -c "make html"
docker run --rm -it -v $(pwd):/doc tarantool/doc-builder sh -c "make singlehtml"
docker run --rm -it -v $(pwd):/doc tarantool/doc-builder sh -c "make pdf"
docker run --rm -it -p 8001:8001 -v $(pwd):/doc tarantool/doc-builder sh -c "make autobuild"
```

Then find documentation in the ``output`` folder .
