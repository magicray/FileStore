import os
import sys
import json
import filestore


def main():
    client = filestore.Client(sys.argv[1].split(','))

    result = client.get(sys.argv[2])
    if not result:
        exit(1)

    blob = result.pop('blob', b'')
    sys.stderr.write(json.dumps(result, indent=4, sort_keys=True) + '\n\n')
    os.write(1, blob)


if '__main__' == __name__:
    main()
