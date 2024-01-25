import sys
from geopephub.cli import app


def main():
    app(prog_name="geopephub")


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        print("Pipeline aborted.")
        sys.exit(1)
