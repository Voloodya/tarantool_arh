#!/usr/bin/env python3
#
# A script to upload tarantool-data-grid configs. Usage:
# ./setconfig.py --url localhost:8080 config-dir

import sys
import os
import argparse
import requests
import json
import io
import zipfile


# Only compress files explicitly whitelisted
def is_whitelisted(filename):
    whitelist = (".lua", ".yml", ".avsc", ".wsdl", ".html", ".json", ".xml")
    if filename.endswith(whitelist):
        return True

    return False


def find_files(dirname):
    matches = []
    for root, dirnames, filenames in os.walk(dirname):
        for filename in filenames:
            matches.append(os.path.join(os.path.relpath(root, dirname), filename))

    return [f for f in matches if is_whitelisted(f)]


def normalize_url(url):
    if not url.startswith("http://") and not url.startswith("https://"):
        return "http://" + url

    return url


def urljoin(*args):
    return "/".join(map(lambda x: str(x).rstrip('/'), args))


def pack(basedir, files):
    if not os.path.exists(basedir):
        return None
    matches = []
    for root, dirnames, filenames in os.walk(basedir):
        for filename in filenames:
            matches.append(os.path.join(os.path.relpath(root, basedir), filename))
    matches = [f for f in matches if any([f.endswith(filename) for filename in files])]
    if len(matches) == 0:
        return None

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as file:
        for filename in matches:
            file.write(os.path.join(basedir, filename), filename)

    output.seek(0)
    return output.getvalue()


def upload(url, data, token=None):
    url = urljoin(url, 'admin/config')
    files = {'file': data}
    headers = token and {'Authorization': 'Bearer ' + token} or {}
    r = requests.put(url, files=files, headers=headers)

    if r.status_code != 200:
        print('Error status code: ' + str(r.status_code))
        try:
            parsed = json.loads(r.text)

            if "str" in parsed:
                print(parsed["str"])
        except Exception:
            print(r.text)

        sys.exit(1)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--url", default="localhost:8080")
    parser.add_argument("--auth-token")
    parser.add_argument("path", default=".", nargs="?")
    args = parser.parse_args()

    files = find_files(args.path)
    url = normalize_url(args.url)

    if "./config.yml" not in files:
        print("Expected config.yml in %s" % args.path)
        sys.exit(1)

    archive = pack(args.path, files)
    upload(url, archive, args.auth_token)


if __name__ == "__main__":
    main()
