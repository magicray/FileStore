import time
import glob
from distutils.core import setup

setup(
  name = 'ObjectStore',
  packages = ['ObjectStore'],
  version = time.strftime('%Y%m%d'),
  description = 'Object Store - with get/put operations over HTTPS.',
  long_description = 'Uses Paxos for replication and mTLS for auth.\nLeaderless and highly available.\nProvides object with versions and atomic updates',
  author = 'Bhupendra Singh',
  author_email = 'bhsingh@gmail.com',
  url = 'https://github.com/magicray/ObjectStore'
)
