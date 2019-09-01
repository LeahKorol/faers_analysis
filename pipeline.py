import luigi
import os

from src import download_faers_data, deduplicate_faers_data


class Faers_Pipeline(luigi.Task):
    dir_data = 'data'
    dir_external = os.path.join(dir_data, 'external')
    dir_interim = os.path.join(dir_data, 'interim')
    dir_processed = os.path.join(dir_data, 'processed')

    year_q_from = luigi.Parameter(default='2013q1')
    year_q_to = luigi.Parameter(default='2013q2')

    def requires(self):
        # download the data
        download = DownloadData(dir_output=self.dir_external, year_q_from=self.year_q_from, year_q_to=self.year_q_to)
        download.run()
        # deduplicate the data
        dedup = DeduplicateData(dir_in=download.output().path, dir_out=os.path.join(self.dir_interim, 'faers_deduplicated'))
        yield dedup
        # mark the data
        # generate the report
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join(self.dir_processed, 'reports'))


class DownloadData(luigi.Task):
    dir_output = luigi.Parameter(default='data/external')
    year_q_from = luigi.Parameter()
    year_q_to = luigi.Parameter()
    threads = luigi.IntParameter(default=4)

    def output(self):
        dir_target = os.path.join(self.dir_output, 'faers')
        return luigi.LocalTarget(dir_target)


    def run(self):
        self.output().makedirs()
        download_faers_data.main(
            year_q_from=self.year_q_from,
            year_q_to=self.year_q_to,
            dir_out=self.output().path,
            threads=self.threads
        )
        assert os.path.exists(self.output().path)


class DeduplicateData(luigi.Task):
    dir_in = luigi.Parameter()
    dir_out = luigi.Parameter()
    threads = luigi.IntParameter(default=4)

    def input(self):
        return luigi.LocalTarget(self.dir_in)

    def output(self):
        return luigi.LocalTarget(self.dir_out)

    def run(self):
        deduplicate_faers_data.main(dir_in=self.input().path, dir_out=self.output().path, threads=self.threads)


if __name__ == '__main__':
    luigi.run(["--local-scheduler"], main_task_cls=Faers_Pipeline)
