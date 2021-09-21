import heapq
import os
import typing

MEMORY_LIMIT_BYTES = 10**5


class ChunkPointer(typing.NamedTuple):
    line: bytes
    filename: str
    file: typing.BinaryIO


def merge_chunks(
        chunk_pointers: typing.List[ChunkPointer],
        merge_level: int,
        chunk_idx: int,
) -> None:
    heapq.heapify(chunk_pointers)
    with open(f'chunk_{merge_level}_{chunk_idx}', 'wb') as sorted_chunk:
        while chunk_pointers:
            min_pointer = heapq.heappop(chunk_pointers)
            sorted_chunk.write(min_pointer.line)
            new_pointer = ChunkPointer(
                min_pointer.file.readline(),
                min_pointer.filename,
                min_pointer.file,
            )
            if not new_pointer.line:
                new_pointer.file.close()
                os.remove(new_pointer.filename)
                continue
            heapq.heappush(chunk_pointers, new_pointer)


def merge_chunks_by_batches(
        merge_level: int, chunks_count: int, batch_size: int,
) -> int:
    chunk_pointers = []
    new_chunks_count = 0
    for i in range(chunks_count):
        chunk_name = f'chunk_{merge_level}_{i}'
        chunk_file = open(chunk_name, 'rb')
        chunk_pointers.append(
            ChunkPointer(chunk_file.readline(), chunk_name, chunk_file),
        )

        if len(chunk_pointers) == batch_size:
            merge_chunks(chunk_pointers, merge_level + 1, new_chunks_count)
            new_chunks_count += 1

    if chunk_pointers:
        merge_chunks(chunk_pointers, merge_level + 1, new_chunks_count)
        new_chunks_count += 1

    return new_chunks_count


def main() -> None:
    chunks_count = 0
    merge_level = 0
    max_line_len = 0

    with open('numbers', 'rb') as numbers:
        numbers_end = numbers.seek(0, 2)

        left_bound = 0
        while left_bound < numbers_end:
            right_bound = min(left_bound + MEMORY_LIMIT_BYTES, numbers_end)
            numbers.seek(right_bound)
            while numbers.read(1) != b'\n':
                right_bound -= 1
                numbers.seek(right_bound)
            numbers.seek(left_bound)
            with open(f'chunk_{merge_level}_{chunks_count}', 'wb') as sorted_chunk:
                sorted_lines = sorted(
                    numbers.read(right_bound - left_bound).split(b'\n'),
                )
                max_line_len = max(max_line_len, max(map(len, sorted_lines)))
                sorted_chunk.write(b'\n'.join(sorted_lines) + b'\n')
                sorted_chunk.flush()

            chunks_count += 1
            left_bound = right_bound + 1

    memory_limit_lines = MEMORY_LIMIT_BYTES // max_line_len
    while chunks_count > 1:
        chunks_count = merge_chunks_by_batches(
            merge_level, chunks_count, memory_limit_lines,
        )
        merge_level += 1

    os.rename(f'chunk_{merge_level}_0', 'numbers_sorted')


if __name__ == '__main__':
    main()
