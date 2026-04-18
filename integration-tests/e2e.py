# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "boto3>=1.42.69",
#     "boto3-stubs[s3]>=1.42.70",
#     "rich>=14.3.3",
#     "testcontainers>=4.14.1",
# ]
# ///

import argparse

from e2e_access_log import main as run_access_log
from e2e_tiering import main as run_tiering
from e2e_utils import info


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="e2e tests",
        description="End-to-end tests for parkhaus",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    tiering = subparsers.add_parser(
        "tiering",
        help="Run age-based tiering and fallback e2e scenario",
    )
    tiering.add_argument(
        "tier_by_bucket",
        default=False,
        metavar="BY_BUCKET",
        help="Whether to test tiering by bucket (true/false)",
    )

    subparsers.add_parser(
        "access-log",
        help="Run access-log driven migration e2e scenario",
    )

    args = parser.parse_args()
    if args.command == "tiering":
        info(f"Running tiering scenario with tier-by-bucket={args.tier_by_bucket}")
        run_tiering(args.tier_by_bucket)
        return

    if args.command == "access-log":
        info("Running access-log scenario")
        run_access_log()
        return

    parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
