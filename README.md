# Tarantool Data Grid: business object storage and routing

[![Tests](https://github.com/tarantool/tdg2/actions/workflows/tests.yml/badge.svg)](https://github.com/tarantool/tdg2/actions/workflows/tests.yml)

## Running in Docker (for demo purposes)

This is helpful to quickly get started with `tdg` without setting up a development environment

```bash
# You will need an access to Mail.ru Cloud Solutions
# to download Tarantool Enterprise
# Choose appropriate Tarantool Enterprise Bundle version

export AWS_ACCESS_KEY_ID=""
export AWS_SECRET_ACCESS_KEY=""

git submodule update --init --recursive

docker build \
      --build-arg BUNDLE_VERSION=$(cat CURRENT_BUNDLE_VERSION) \
      --build-arg AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
      --build-arg AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
      --target production \
      -t tdg \
      -f Dockerfile.build .

docker run --rm -t -i -p8080:8080 tdg
```

Then point your browser at http://localhost:8080

## Running natively (for development)

Dependencies:
- [Tarantool Enterprise 2.8.2+](https://tarantool.io/)
- [Python3 3.6+](https://www.python.org/downloads/) for building docs and running tests
- [Node.js and npm](https://www.npmjs.com/get-npm) for building frontend

To start development server for the first time, say that:

```bash
git submodule update --init --recursive
sudo pip3 install -r doc/requirements.txt
sudo pip3 install -r test/requirements.txt
cartridge build
```

The commands above are also desirable after switching to a different git branch.

To start the server, run

```bash
./init.lua --bootstrap true
```

This will bring up the server with configuration from `config.yml` and start
listening on http://localhost:8080.

The following runs do not require bootstrapping:
configuration is saved in tarantool snapshots, which are located in `./dev/output`
by default. If you want to start over and drop the database, run:

```bash
rm -rf ./dev/output
```

If you have a config you want to upload (say, to `localhost:8080`), do the following:

```bash
./setconfig.py --url localhost:8080 config-dir
```

## Build rpm or tgz from source

If you want to get `rpm` or `tgz` archive with TDG distributive.
You can simply use `make_package.sh` script.

```sh
export AWS_ACCESS_KEY_ID=""
export AWS_SECRET_ACCESS_KEY=""

./make_package.sh rpm   # To pack TDG to rpm
./make_package.sh tgz   # To pack TDG to tgz
```
