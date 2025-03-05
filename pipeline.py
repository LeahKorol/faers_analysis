import os

import luigi

from src import (
    download_faers_data,
    deduplicate_faers_data,
    mark_data,
    get_demographic_data,
    summarize_demographic_data,
    report,
)


class Faers_Pipeline(luigi.Task):
    dir_data = "data"
    dir_external = os.path.join(dir_data, "external")
    dir_interim = os.path.join(dir_data, "interim")
    dir_processed = os.path.join(dir_data, "processed")
    config_dir = "config"

    dir_out_report = dir_processed

    year_q_from = luigi.Parameter(default="2013q1")
    year_q_to = luigi.Parameter(default="2023q1")

    def requires(self):
        # download the data
        download = DownloadData(
            dir_output=self.dir_external,
            year_q_from=self.year_q_from,
            year_q_to=self.year_q_to,
        )
        yield download

        # deduplicate the data
        dedup = DeduplicateData(
            dir_in=download.output().path,
            dir_out=os.path.join(self.dir_interim, "faers_deduplicated"),
            dependency_params={"download": download.param_kwargs},
        )
        yield dedup
        # mark the data
        marked = MarkTheData(
            year_q_from=download.year_q_from,
            year_q_to=download.year_q_to,
            dir_in=dedup.output().path,
            config_dir=self.config_dir,
            dir_out=os.path.join(self.dir_interim, "marked_data"),
            dependency_params={"deduplicate": dedup.param_kwargs},
        )
        yield marked

        demographic_data = GetDemographicData(
            year_q_from=download.year_q_from,
            year_q_to=download.year_q_to,
            dir_marked_data=marked.output().path,
            dir_raw_data=dedup.output().path,
            dir_config=self.config_dir,
            dir_out=os.path.join(self.dir_interim, "demographic_analysis"),
            clean_on_failure=True,
            dependency_params={"mark_the_data": marked.param_kwargs},
            threads=1,
        )
        yield demographic_data

        demographic_summary = SummarizeDemographicData(
            dir_demography_data=demographic_data.output().path,
            dir_config=self.config_dir,
            dir_out=os.path.join(self.dir_interim, "demographic_summary"),
            clean_on_failure=True,
            dependency_params={"get_demographic_data": demographic_data.param_kwargs},
        )
        yield demographic_summary

        yielded_report = Report(
            dir_marked_data=marked.output().path,
            dir_raw_data=dedup.output().path,
            config_dir=self.config_dir,
            dir_reports=self.output().path,
            output_raw_exposure_data=True,
            dependency_params={
                "mark_the_data": marked.param_kwargs,
                "deduplicate": dedup.param_kwargs,
            },
        )
        yield yielded_report

    def output(self):
        return luigi.LocalTarget(os.path.join(self.dir_processed, "reports"))


class DownloadData(luigi.Task):
    dir_output = luigi.Parameter()
    year_q_from = luigi.Parameter()
    year_q_to = luigi.Parameter()
    threads = luigi.IntParameter(default=4)

    def output(self):
        dir_target = os.path.join(self.dir_output, "faers")
        return luigi.LocalTarget(dir_target)

    def run(self):
        self.output().makedirs()
        download_faers_data.main(
            year_q_from=self.year_q_from,
            year_q_to=self.year_q_to,
            dir_out=self.output().path,
            threads=self.threads,
        )
        assert os.path.exists(self.output().path)


class DeduplicateData(luigi.Task):
    dir_in = luigi.Parameter()
    dir_out = luigi.Parameter()
    threads = luigi.IntParameter(default=4)
    dependency_params = luigi.DictParameter()

    def requires(self):
        return [DownloadData(**(self.dependency_params["download"]))]

    def input(self):
        return luigi.LocalTarget(self.dir_in)

    def output(self):
        return luigi.LocalTarget(self.dir_out)

    def run(self):
        deduplicate_faers_data.main(
            dir_in=self.input().path, dir_out=self.output().path, threads=self.threads
        )


class MarkTheData(luigi.Task):
    year_q_from = luigi.Parameter()
    year_q_to = luigi.Parameter()
    dir_in = luigi.Parameter()
    config_dir = luigi.Parameter()
    dir_out = luigi.Parameter()
    threads = luigi.IntParameter(default=7)
    dependency_params = luigi.DictParameter()

    def requires(self):
        return [DeduplicateData(**self.dependency_params["deduplicate"])]

    def input(self):
        return [luigi.LocalTarget(self.dir_in), luigi.LocalTarget(self.config_dir)]

    def output(self):
        return luigi.LocalTarget(self.dir_out)

    def run(self):
        mark_data.main(
            year_q_from=self.year_q_from,
            year_q_to=self.year_q_to,
            dir_in=self.input()[0].path,
            config_dir=self.input()[1].path,
            dir_out=self.output().path,
            threads=self.threads,
        )


class GetDemographicData(luigi.Task):
    year_q_from = luigi.Parameter()
    year_q_to = luigi.Parameter()
    dir_marked_data = luigi.Parameter()
    dir_raw_data = luigi.Parameter()
    dir_config = luigi.Parameter()
    dir_out = luigi.Parameter()
    threads = luigi.IntParameter(default=4)
    clean_on_failure = luigi.BoolParameter(default=True)
    dependency_params = luigi.DictParameter(default=dict())

    def requires(self):
        return [MarkTheData(**self.dependency_params["mark_the_data"])]

    def input(self):
        return [
            luigi.LocalTarget(self.dir_raw_data),
            luigi.LocalTarget(self.dir_marked_data),
            luigi.LocalTarget(self.dir_config),
        ]

    def output(self):
        return luigi.LocalTarget(self.dir_out)

    def run(self):
        dir_raw_data, dir_marked_data, dir_config = [inp.path for inp in self.input()]
        get_demographic_data.main(
            year_q_from=self.year_q_from,
            year_q_to=self.year_q_to,
            dir_raw_data=dir_raw_data,
            dir_marked_data=dir_marked_data,
            dir_config=dir_config,
            dir_out=self.output().path,
            threads=self.threads,
            clean_on_failure=self.clean_on_failure,
        )


class SummarizeDemographicData(luigi.Task):
    dir_demography_data = luigi.Parameter()
    dir_config = luigi.Parameter()
    dir_out = luigi.Parameter()
    clean_on_failure = luigi.BoolParameter(default=True)
    dependency_params = luigi.DictParameter()

    def requires(self):
        return [GetDemographicData(**self.dependency_params["get_demographic_data"])]

    def input(self):
        return [
            luigi.LocalTarget(self.dir_demography_data),
            luigi.LocalTarget(self.dir_config),
        ]

    def output(self):
        return luigi.LocalTarget(self.dir_out)

    def run(self):
        dir_demography_data, dir_config = [inp.path for inp in self.input()]
        summarize_demographic_data.main(
            dir_demography_data=dir_demography_data,
            dir_config=dir_config,
            dir_out=self.dir_out,
            clean_on_failure=self.clean_on_failure,
        )


class Report(luigi.Task):
    dir_marked_data = luigi.Parameter()
    dir_raw_data = luigi.Parameter()
    config_dir = luigi.Parameter()
    dir_reports = luigi.Parameter()
    output_raw_exposure_data = luigi.BoolParameter()
    dependency_params = luigi.DictParameter()

    def requires(self):
        return [
            MarkTheData(**self.dependency_params["mark_the_data"]),
            DeduplicateData(**self.dependency_params["deduplicate"]),
        ]

    def input(self):
        return [
            luigi.LocalTarget(self.dir_marked_data),
            luigi.LocalTarget(self.dir_raw_data),
            luigi.LocalTarget(self.config_dir),
        ]

    def output(self):
        return luigi.LocalTarget(self.dir_reports)

    def run(self):
        dir_marked_data, dir_raw_data, config_dir = [inp.path for inp in self.input()]
        report.main(
            dir_marked_data=dir_marked_data,
            dir_raw_data=dir_raw_data,
            config_dir=config_dir,
            dir_reports=self.output().path,
            output_raw_exposure_data=self.output_raw_exposure_data,
        )


if __name__ == "__main__":
    luigi.run(
        [
            "--local-scheduler",
            "--Faers-Pipeline-year-q-from",
            "2015q1",
            "--Faers-Pipeline-year-q-to",
            "2022q4",
        ],
        main_task_cls=Faers_Pipeline,
    )
