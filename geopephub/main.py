import sys
from geopephub.cli import app
from geopephub.const import __name__


def main():
    app(prog_name=__name__)


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        print("Pipeline aborted.")
        sys.exit(1)
