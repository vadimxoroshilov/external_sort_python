import dataclasses
import typing

MEMORY_LIMIT_BYTES = 10**3
HALF_MEMORY_LIMIT_BYTES = MEMORY_LIMIT_BYTES // 2


class SplitSeriesWriter:
    def __init__(self, file_1: typing.BinaryIO, file_2: typing.BinaryIO) -> None:
        self._file_1 = file_1
        self._file_2 = file_2
        self._write_to_first_file = True

    def switch_file(self) -> None:
        self._write_to_first_file = not self._write_to_first_file

    def write_and_flush(self, s: bytes) -> None:
        if self._write_to_first_file:
            self._file_1.write(s)
            self._file_1.flush()
            return
        self._file_2.write(s)
        self._file_2.flush()


class IncreasingSeriesReader:
    @dataclasses.dataclass
    class Element:
        series_number: int
        element: bytes

    def __init__(self, file: typing.BinaryIO) -> None:
        self._file = file

        file.seek(0, 2)
        self._file_end = file.tell()
        file.seek(0)

        self._current_series_number = 0
        self._chunk_left_bound = 0
        self._chunk_right_bound = 0

        self._cached_element_idx = 0
        self._cached_split_chunk = []

    def get_next_element(self) -> typing.Optional[Element]:
        if (
                self._chunk_left_bound >= self._file_end and
                self._cached_element_idx >= len(self._cached_split_chunk)
        ):
            return None
        if self._cached_element_idx >= len(self._cached_split_chunk):
            self._upload_new_chunk()

        elem = IncreasingSeriesReader.Element(
            self._current_series_number,
            self._cached_split_chunk[self._cached_element_idx],
        )

        self._cached_element_idx += 1
        if (
                self._cached_element_idx < len(self._cached_split_chunk) and
                self._cached_split_chunk[self._cached_element_idx] == ''
        ):
            self._current_series_number += 1
            self._cached_element_idx += 1

        return elem

    def _upload_new_chunk(self) -> None:
        self._file.seek(self._chunk_left_bound)
        next_2_bytes = self._file.read(2)
        if next_2_bytes == b'\n\n':
            self._current_series_number += 1
            self._chunk_left_bound += 2
        elif next_2_bytes[0] == b'\n':
            self._chunk_left_bound += 1

        self._chunk_right_bound = self._chunk_left_bound + HALF_MEMORY_LIMIT_BYTES
        if self._file_end <= self._chunk_right_bound + 1:
            self._file.seek(self._chunk_left_bound)
            self._cached_split_chunk = self._file.read(self._file_end - self._chunk_left_bound).split()
            self._chunk_left_bound = self._file_end
            self._cached_element_idx = 0
            return

        self._file.seek(self._chunk_right_bound)
        while self._file.read(1) != b'\n':
            self._chunk_right_bound -= 1
            self._file.seek(self._chunk_right_bound)
        self._file.seek(self._chunk_right_bound)
        while self._file.read(1) == b'\n':
            self._chunk_right_bound -= 1
            self._file.seek(self._chunk_right_bound)
        self._file.seek(self._chunk_left_bound)
        self._cached_split_chunk = self._file.read(self._chunk_right_bound - self._chunk_left_bound + 1).split()
        self._chunk_left_bound = self._chunk_right_bound + 1
        self._cached_element_idx = 0


def split_increasing_series(
        target: typing.BinaryIO, buffer_1: typing.BinaryIO, buffer_2: typing.BinaryIO,
) -> None:
    buffer_1.truncate(0)
    buffer_2.truncate(0)
    buffer_1.seek(0)
    buffer_2.seek(0)
    writer = SplitSeriesWriter(buffer_1, buffer_2)

    target.seek(0, 2)
    end_pos = target.tell()
    target.seek(0)

    left_pos = 0
    last_chunk_elem = None
    while left_pos < end_pos:
        right_pos = left_pos + MEMORY_LIMIT_BYTES
        target.seek(right_pos)
        target.seek(right_pos)
        while target.read(1) != b'\n':
            right_pos -= 1
            target.seek(right_pos)

        bytes_to_read = right_pos - left_pos
        target.seek(left_pos)
        chunk = target.read(bytes_to_read).split(b'\n')

        if last_chunk_elem is not None:
            writer.write_and_flush(last_chunk_elem + b'\n')
            if last_chunk_elem > chunk[0]:
                writer.write_and_flush(b'\n')
                writer.switch_file()

        for i in range(len(chunk) - 1):
            writer.write_and_flush(chunk[i] + b'\n')
            if chunk[i] > chunk[i + 1]:
                writer.write_and_flush(b'\n')
                writer.switch_file()

        last_chunk_elem = chunk[-1]

        left_pos = right_pos + 1

    writer.write_and_flush(last_chunk_elem + b'\n\n')

    buffer_1.seek(0, 2)
    buffer_end_1 = max(0, buffer_1.tell() - 2)
    buffer_1.seek(buffer_end_1)
    buffer_1.truncate()

    buffer_2.seek(0, 2)
    buffer_end_2 = max(0, buffer_2.tell() - 2)
    buffer_2.seek(buffer_end_2)
    buffer_2.truncate()


def merge_increasing_series(
        target: typing.BinaryIO, buffer_1: typing.BinaryIO, buffer_2: typing.BinaryIO,
) -> None:
    target.seek(0)
    buffer_1.seek(0)
    buffer_2.seek(0)

    reader_1 = IncreasingSeriesReader(buffer_1)
    reader_2 = IncreasingSeriesReader(buffer_2)

    elem_1 = reader_1.get_next_element()
    elem_2 = reader_2.get_next_element()

    while elem_1 or elem_2:
        if not elem_1:
            target.write(elem_2.element + b'\n')
            target.flush()
            elem_2 = reader_2.get_next_element()
            continue
        if not elem_2:
            target.write(elem_1.element + b'\n')
            target.flush()
            elem_1 = reader_1.get_next_element()
            continue
        if elem_1.series_number < elem_2.series_number:
            target.write(elem_1.element + b'\n')
            target.flush()
            elem_1 = reader_1.get_next_element()
            continue
        if elem_1.series_number > elem_2.series_number:
            target.write(elem_2.element + b'\n')
            target.flush()
            elem_2 = reader_2.get_next_element()
            continue
        if elem_1.element <= elem_2.element:
            target.write(elem_1.element + b'\n')
            target.flush()
            elem_1 = reader_1.get_next_element()
            continue
        target.write(elem_2.element + b'\n')
        target.flush()
        elem_2 = reader_2.get_next_element()
        continue


def main() -> None:
    with open('numbers', 'rb') as numbers, open('buffer', 'wb') as buffer:
        chunk = numbers.read(MEMORY_LIMIT_BYTES)
        while chunk:
            buffer.write(chunk)
            chunk = numbers.read(MEMORY_LIMIT_BYTES)

    with open('buffer', 'rb+') as buffer, open('buffer_1', 'wb') as buffer_1, open('buffer_2', 'wb') as buffer_2:
        split_increasing_series(buffer, buffer_1, buffer_2)

    with open('buffer', 'rb+') as buffer, open('buffer_1', 'rb+') as buffer_1, open('buffer_2', 'rb+') as buffer_2:
        buffer_2.seek(0)
        while buffer_2.read(1):
            merge_increasing_series(buffer, buffer_1, buffer_2)
            split_increasing_series(buffer, buffer_1, buffer_2)
            buffer_2.seek(0)

    with open('buffer', 'rb') as buffer, open('numbers_sorted', 'wb') as numbers_sorted:
        chunk = buffer.read(MEMORY_LIMIT_BYTES)
        while chunk:
            numbers_sorted.write(chunk)
            chunk = buffer.read(MEMORY_LIMIT_BYTES)


if __name__ == '__main__':
    main()
