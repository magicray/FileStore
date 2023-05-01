import sys
import json
import filestore


def main():
    client = filestore.Client(sys.argv[1].split(','))

    result = client.put(sys.argv[2], sys.stdin.read())
    if not result:
        exit(1)

    print(json.dumps(result, indent=4, sort_keys=True))


if '__main__' == __name__:
    main()
