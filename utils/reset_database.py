#!/usr/bin/env python3
"""Database reset utility for StockLens.

This script provides functionality to reset the database and clear all analysis data.
"""

import argparse
import sys
from pathlib import Path
import shutil


class DatabaseReset:
    """Handles database and data reset operations."""

    def __init__(self, project_root: Path = None):
        """
        Initialize DatabaseReset utility.

        Args:
            project_root: Path to project root directory
        """
        if project_root is None:
            project_root = Path(__file__).parent.parent

        self.project_root = Path(project_root)
        self.data_dir = self.project_root / "data"
        self.dashboard_dir = self.project_root / "dashboard"
        self.db_file = self.data_dir / "stocklens.db"

    def reset_database(self) -> bool:
        """
        Delete the SQLite database file.

        Returns:
            True if successful, False otherwise
        """
        if self.db_file.exists():
            try:
                self.db_file.unlink()
                print(f"✓ Database deleted: {self.db_file}")
                return True
            except Exception as e:
                print(f"✗ Error deleting database: {e}")
                return False
        else:
            print(f"ℹ Database not found: {self.db_file}")
            return True

    def reset_raw_data(self) -> bool:
        """
        Delete all raw parquet files.

        Returns:
            True if successful, False otherwise
        """
        raw_dir = self.data_dir / "raw"
        if not raw_dir.exists():
            print(f"ℹ Raw data directory not found: {raw_dir}")
            return True

        try:
            deleted_count = 0
            for file in raw_dir.glob("*.parquet"):
                file.unlink()
                deleted_count += 1

            print(f"✓ Deleted {deleted_count} raw data file(s)")
            return True
        except Exception as e:
            print(f"✗ Error deleting raw data: {e}")
            return False

    def reset_processed_data(self) -> bool:
        """
        Delete all processed parquet and JSON files.

        Returns:
            True if successful, False otherwise
        """
        processed_dir = self.data_dir / "processed"
        if not processed_dir.exists():
            print(f"ℹ Processed data directory not found: {processed_dir}")
            return True

        try:
            deleted_count = 0
            for pattern in ["*.parquet", "*.json"]:
                for file in processed_dir.glob(pattern):
                    file.unlink()
                    deleted_count += 1

            print(f"✓ Deleted {deleted_count} processed data file(s)")
            return True
        except Exception as e:
            print(f"✗ Error deleting processed data: {e}")
            return False

    def reset_dashboard(self) -> bool:
        """
        Delete all generated dashboard HTML files.

        Returns:
            True if successful, False otherwise
        """
        if not self.dashboard_dir.exists():
            print(f"ℹ Dashboard directory not found: {self.dashboard_dir}")
            return True

        try:
            deleted_count = 0
            for file in self.dashboard_dir.glob("*.html"):
                file.unlink()
                deleted_count += 1

            print(f"✓ Deleted {deleted_count} dashboard file(s)")
            return True
        except Exception as e:
            print(f"✗ Error deleting dashboard: {e}")
            return False

    def reset_all(self) -> bool:
        """
        Reset everything: database, raw data, processed data, and dashboard.

        Returns:
            True if all operations successful, False otherwise
        """
        print("\n🔄 Resetting StockLens data...")
        print("=" * 60)

        success = True
        success &= self.reset_database()
        success &= self.reset_raw_data()
        success &= self.reset_processed_data()
        success &= self.reset_dashboard()

        print("=" * 60)
        if success:
            print("✅ Reset completed successfully")
        else:
            print("⚠️  Reset completed with some errors")

        return success


def main():
    """Main entry point for the reset utility."""
    parser = argparse.ArgumentParser(
        description="Reset StockLens database and analysis data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Reset everything (database, raw data, processed data, dashboard)
  python utils/reset_database.py --all

  # Reset only the database
  python utils/reset_database.py --database

  # Reset only raw data files
  python utils/reset_database.py --raw

  # Reset only processed data files
  python utils/reset_database.py --processed

  # Reset only dashboard files
  python utils/reset_database.py --dashboard

  # Reset multiple components
  python utils/reset_database.py --database --processed --dashboard
        """
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Reset everything (database, raw data, processed data, dashboard)"
    )
    parser.add_argument(
        "--database",
        action="store_true",
        help="Reset SQLite database only"
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Reset raw data files only"
    )
    parser.add_argument(
        "--processed",
        action="store_true",
        help="Reset processed data files only"
    )
    parser.add_argument(
        "--dashboard",
        action="store_true",
        help="Reset dashboard files only"
    )
    parser.add_argument(
        "--yes", "-y",
        action="store_true",
        help="Skip confirmation prompt"
    )

    args = parser.parse_args()

    # If no specific flags, show help
    if not any([args.all, args.database, args.raw, args.processed, args.dashboard]):
        parser.print_help()
        sys.exit(1)

    # Confirmation prompt
    if not args.yes:
        print("\n⚠️  WARNING: This will permanently delete data!")
        print("Components to reset:")
        if args.all:
            print("  • Database")
            print("  • Raw data")
            print("  • Processed data")
            print("  • Dashboard")
        else:
            if args.database:
                print("  • Database")
            if args.raw:
                print("  • Raw data")
            if args.processed:
                print("  • Processed data")
            if args.dashboard:
                print("  • Dashboard")

        response = input("\nContinue? (yes/no): ").strip().lower()
        if response not in ["yes", "y"]:
            print("Cancelled.")
            sys.exit(0)

    # Perform reset
    resetter = DatabaseReset()

    if args.all:
        success = resetter.reset_all()
    else:
        print("\n🔄 Resetting StockLens data...")
        print("=" * 60)
        success = True

        if args.database:
            success &= resetter.reset_database()
        if args.raw:
            success &= resetter.reset_raw_data()
        if args.processed:
            success &= resetter.reset_processed_data()
        if args.dashboard:
            success &= resetter.reset_dashboard()

        print("=" * 60)
        if success:
            print("✅ Reset completed successfully")
        else:
            print("⚠️  Reset completed with some errors")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
