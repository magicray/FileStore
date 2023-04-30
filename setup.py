import time
from distutils.core import setup

setup(
  name = 'filestore',
  packages = ['filestore'],
  version = time.strftime('%Y%m%d'),
  description = 'File Store - with get/put operations over HTTPS.',
  long_description = 'Uses Paxos for replication and mTLS for auth.\nLeaderless and highly available.\nProvides object with versions and atomic updates',
  author = 'Bhupendra Singh',
  author_email = 'bhsingh@gmail.com',
  url = 'https://github.com/magicray/filestore'
)
