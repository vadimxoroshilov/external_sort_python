import argparse
import random
import string


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--lines_count', type=int, required=True)
    parser.add_argument('--line_max_size', type=int, required=True)
    args = parser.parse_args()

    with open('numbers', 'wb') as numbers:
        numbers.write(
            b'\n'.join(
                ''.join(
                    random.choices(
                        string.ascii_lowercase, k=random.randint(1, args.line_max_size)
                    )
                ).encode('utf-8') for i in range(args.lines_count)
            ) + b'\n'
        )


if __name__ == '__main__':
    main()
