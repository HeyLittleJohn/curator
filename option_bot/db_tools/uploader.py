from typing import Awaitable


class Uploader:
    def __init__(self, upload_func: Awaitable, expected_records: int, record_size: int):
        self.clean_data = []
        self.batch_max = 60000
        self.record_size = record_size  # number of fields per record
        self.upload_func = upload_func
        self.expected_records = expected_records
        self.record_counter = 0
        self.batch_size = self.batch_max // self.record_size

    async def process_clean_data(self, clean_data: list[dict]):
        self.clean_data.append(clean_data)
        self.record_counter += 1
        if self.batch_ready():
            await self.upload_func(self.clean_data)
            self.clean_data = []

    def batch_ready(self) -> bool:
        """return True if the batch is ready to be uploaded.
        A batch is ready when the len of self.clean_data is equal to self.batch_size.
        Or when there are no more records expected and the len of self.clean_data is greater than 0"""
        ready = False
        if len(self.clean_data) == self.batch_size:
            ready = True
        elif self.record_counter == self.expected_records and len(self.clean_data) > 0:
            ready = True
        return ready

    def update_expected_records(self):
        """decrease the expected number of records if the query returns no data"""
        self.expected_records -= 1
