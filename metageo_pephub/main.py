from cli import _parse_cmdl
import sys
from metageo_pephub import metageo_main


def main():
    args_dict = vars(_parse_cmdl(sys.argv[1:]))
    metageo_main(**args_dict)


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        print("Pipeline aborted.")
        sys.exit(1)
