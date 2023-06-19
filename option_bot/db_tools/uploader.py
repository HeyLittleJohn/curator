from typing import Awaitable

from option_bot.proj_constants import log


class Uploader:
    def __init__(self, upload_func: Awaitable, expected_args: int, record_size: int):
        self.clean_data = []
        self.batch_max = 62000
        self.record_size = record_size  # number of fields per record
        self.upload_func = upload_func
        self.expected_args = expected_args
        self.arg_counter = 0
        self.batch_counter = 1
        self.batch_size = self.batch_max // self.record_size

    async def process_clean_data(self, clean_data: list[dict]):
        """adds the clean data to the uploader object which uploads in batches based on the amount of data present"""
        log.info(f"received clean results for arg number {self.arg_counter}")
        self.clean_data.extend(clean_data)
        if self.arg_counter == 0:
            self._update_record_size(clean_data)
        self.arg_counter += 1  # index of 1 since matching len of url_args
        if len(self.clean_data) >= self.batch_size or self.arg_counter == self.expected_args:
            for batch in self._make_batch_generator():
                log.info(f"uploading batch {self.batch_counter} with {self.upload_func.__qualname__}")
                await self.upload_func(batch)
                self.batch_counter += 1

    def update_expected_args(self):
        """decrease the expected number of pool args if the query returns no data"""
        self.expected_args -= 1

    # NOTE: expected_records is originally set to the number of args put into the pool

    def _update_record_size(self, clean_data: list[dict]):
        """update the record size if the first arg returns data"""
        self.record_size = len(clean_data[0].keys())
        self.batch_size = self.batch_max // self.record_size

    def _make_batch_generator(self):
        """generator that batches self.clean_data into batches of size self.batch_size"""
        try:
            for i in range(0, len(self.clean_data), self.batch_size):
                yield self.clean_data[i : i + self.batch_size]

        except Exception as e:
            log.exception(e)
            raise e

        finally:
            self.clean_data = []  # return to empty list after all data has been uploaded
            self.batch_counter = 1
