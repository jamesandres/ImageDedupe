import os
import sys
import magic
import json
import multiprocessing
import logging
from PIL import Image

# For example set the environment variable LOG_LEVEL=10 for debug level logging.
logging.basicConfig(level=int(os.environ.get('LOG_LEVEL', logging.INFO)))
logger = logging.getLogger(__name__)


class ImageHashProcess(multiprocessing.Process):
    def __init__(self, id, lock, dedupe_map, jobs):
        super(ImageHashProcess, self).__init__()

        self.id = id
        self.lock = lock
        self.dedupe_map = dedupe_map
        self.jobs = jobs

    def run(self):
        logger.debug("-> Running ImageHashProcess #%d on %d jobs" % (
            self.id, len(self.jobs)))
        logger.debug("   id(lock)       = %s" % id(self.lock))
        logger.debug("   id(dedupe_map) = %s" % id(self.dedupe_map))
        logger.debug("   id(jobs)       = %s" % id(self.jobs))

        for filepath in self.jobs:
            hash, size = self.hash_image(filepath)

            logger.debug("    ... #%d Done processing %s %s" % (
                self.id, hash, size))
            with self.lock:
                self.update_dedupe_map(hash, filepath, size)
                logger.debug("    ... #%d Updated map has %d items" % (
                    self.id, len(self.dedupe_map)))

    def hash_image(self, filepath):
        """
        See: http://blog.safariflow.com/2013/11/26/image-hashing-with-python/
        """
        image = Image.open(filepath)
        size = image.size
        image = image.resize((8, 8), Image.ANTIALIAS)
        image = image.convert("L")
        pixels = list(image.getdata())
        avg = sum(pixels) / len(pixels)
        bits = "".join(map(lambda pixel: '1' if pixel < avg else '0', pixels))
        return int(bits, 2).__format__('016x'), size

    def update_dedupe_map(self, hash, filepath, size):
        this_item = {
            'filepath': filepath,
            'size': size,
            'area': size[0] * size[1],
        }

        if hash not in self.dedupe_map:
            self.dedupe_map[hash] = {
                'dupes': [],
                'largest': this_item
            }
            return True

        item = self.dedupe_map[hash]

        if this_item['area'] > item['largest']['area']:
            self.dedupe_map[hash]['dupes'].append(item['largest'])
            self.dedupe_map[hash]['largest'] = this_item
        else:
            self.dedupe_map[hash]['dupes'].append(this_item)


class ImageDedupe(object):
    def __init__(self):
        super(ImageDedupe, self).__init__()

        self.num_workers = multiprocessing.cpu_count()
        self.worker_manager = multiprocessing.Manager()
        self.dedupe_map = self.worker_manager.dict()
        self.workers = []
        self.lock = multiprocessing.Lock()

    def run(self):
        if len(sys.argv) < 2:
            print "Usage: %s DIRECTORY" % os.path.basename(sys.argv[0])
            sys.exit(1)

        self.dedupe_all_images(sys.argv[1])

        print json.dumps(dict(self.dedupe_map))

    def dedupe_all_images(self, directory):
        jobs = []

        for dirname, dirnames, filenames in os.walk(directory):
            for filename in filenames:
                filepath = os.path.join(dirname, filename)

                try:
                    m = magic.from_file(filepath, mime=True)
                    if m.split('/')[0] != 'image':
                        continue
                except IOError:
                    continue

                jobs.append(filepath)

        batch_size = len(jobs) / self.num_workers

        for i in range(0, len(jobs), batch_size):
            jobs_batch = jobs[i:i + batch_size]

            p = ImageHashProcess(i, self.lock, self.dedupe_map, jobs_batch)
            p.start()
            self.workers.append(p)

        for w in self.workers:
            w.join()

idd = ImageDedupe()
idd.run()
