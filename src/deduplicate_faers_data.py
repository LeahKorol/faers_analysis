import glob
import logging
import os
import shutil
from multiprocessing.dummy import Pool as ThreadPool

import defopt
import tqdm

logger = logging.getLogger("FAERS")


def deduplicate_file(file_in, dir_out):
    fn = os.path.split(file_in)[-1]
    file_out = os.path.join(dir_out, fn)
    if os.path.exists(file_out):
        logger.debug(f"Skipping {file_in} because {file_out} already exists")
        return
    open(file_out, "wb").write(open(file_in, "rb").read())


def main(*, dir_in, dir_out, threads=4):
    """

    :param str dir_in:
        Input directory
    :param str dir_out:
        Output directory
    :param int threads:
        N of parallel threads

    :return: None

    """

    assert os.path.isdir(dir_in)
    dir_out = os.path.abspath(dir_out)
    os.makedirs(dir_out, exist_ok=True)
    try:
        in_files = glob.glob(os.path.join(dir_in, "*.*"))
        with ThreadPool(threads) as pool:
            _ = list(
                tqdm.tqdm(
                    pool.imap(
                        lambda file_in: deduplicate_file(file_in, dir_out), in_files
                    ),
                    total=len(in_files),
                )
            )
    except Exception as err:
        shutil.rmtree(dir_out)
        raise err


if __name__ == "__main__":
    defopt.run(main)
