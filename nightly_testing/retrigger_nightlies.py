#!/usr/bin/env python3
# *****************************COPYRIGHT*******************************
# (C) Crown copyright Met Office. All rights reserved.
# For further details please refer to the file COPYRIGHT.txt
# which you should have received as part of this distribution.
# *****************************COPYRIGHT*******************************

"""Scan a cylc-run directory for suites with uncompleted tasks.  Restart
each suite, wait for the command to complete, and retrigger any failed
or submit-failed tasks.

Optionally, suite names or partial suite names can be specified as
positional arguments to limit the scan to a subset of targets.
"""


import os
import re
import sqlite3
import subprocess
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep

import contextlib

try:
    from rich.console import Console
    from rich.table import Table
    from rich.prompt import Confirm
    from rich.progress import track, Progress
    from rich.traceback import install

    install(show_locals=True)

    RICH_SUPPORT = True
except ImportError:
    RICH_SUPPORT = False


class TestSuite:
    """Class containing details about a single TestSuite."""

    # Setting the full path to cylc makes using sudo easier
    cylc_cmd = "/usr/local/bin/cylc"

    def __init__(self, suite, cylc_run):
        self.suite = suite
        self.suite_dir = Path(cylc_run) / suite / "runN"
        self.conn = None
        self.failed_tasks = []
        self.succeeded = 0
        self.restarted = False

        self.connect_to_database(self.suite_dir)
        if self.conn is None or not self.check_for_workflow_params:
            return

        self.check_for_failed_tasks()

    def connect_to_database(self, suite_dir):
        """
        Make a connection to the suite database
        """
        db_filename = suite_dir / "log" / "db"
        if not db_filename.is_file():
            print(f"Warning: Suite database not found at {db_filename}")
        self.conn = sqlite3.connect(db_filename)

    def check_for_workflow_params(self):
        """
        Takes in Conn, a DB connection object and searches the DB for a
        "workflow_params" table - if it exists return True.
        This is used as a proxy for whether a suite is cylc8 - if return True, it is
        """
        res = self.conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='workflow_params'"
        ).fetchall()
        return len(res) > 0

    def check_for_failed_tasks(self):
        """
        Search for tasks in table "task_state" with failed or submit-failed status
        Return a list of these tasks
        """
        for task in self.conn.execute("SELECT name, status FROM task_states"):
            if task[-1] in ("failed", "submit-failed"):
                self.failed_tasks.append(task)
            elif task[-1] == "succeeded":
                self.succeeded += 1

    def restart(self, dry_run):
        """
        Generate and run the command to restart the suite
        """
        play_command = f"{self.cylc_cmd} play -q {self.suite}"
        if "next-cylc" in self.suite:
            play_command = f"export CYLC_VERSION=8-next ; {play_command}"
        run_command(play_command, dry_run)
        self.restarted = True

    def retrigger(self, dry_run, advance=None):
        """
        Generate and run commands to retrigger failed and submit-failed tasks
        """
        for i, task in enumerate(self.failed_tasks, 1):
            if advance is not None:
                advance(i, self.failed)
            failed_command = f"{self.cylc_cmd} trigger {self.suite}//*/{task[0]}"
            if "next-cylc" in self.suite:
                failed_command = f"export CYLC_VERSION=8-next ; {failed_command}"
            run_command(failed_command, dry_run)

    def __str__(self):
        return self.suite

    @property
    def failed(self):
        """Number of failed tasks in the suite."""
        return len(self.failed_tasks)


class NightlyTesting:
    """Class to scan the nightly tests for failures."""

    def __init__(self, cylc_run, restart_names):
        self.cylc_run = Path(cylc_run).absolute()
        self.restart_names = restart_names
        self.failed_suites = []
        self.total_failed_tasks = 0

        # Get the current date
        self.today = datetime.now()
        # If Monday then include suites from the weekend, otherwise just today
        if self.today.weekday() == 0:
            self.day_delta = 2
        else:
            self.day_delta = 1

    def find_testsuites(self):
        """Find test suites and failed tasks in each one."""
        for path in self.cylc_run.iterdir():
            suite = path.name
            if self.check_suite_valid(suite):
                suite = TestSuite(suite, path.parent)
                if suite.failed > 0:
                    self.failed_suites.append(suite)

    def check_suite_valid(self, suite):
        """
        Check whether a suite is valid for a restart - return True if so.
        Valid if from last testing cycle (weekend or nightly) and in restart_names
        if this is defined.
        """
        # If restart names are defined then check whether this suite matches any
        # If not found then return False and don't restart the suite
        # The else will get executed if the for loop hasn't been broken out of -
        # ie. when no suite in restart_names matches the current suite being
        # looked at.
        if self.restart_names:
            for wanted in self.restart_names:
                if wanted in suite:
                    break
            else:
                return False
        # Get the date string from the suite name - if not present then skip
        try:
            date_str = re.findall(r"\d{4}-\d{2}-\d{2}", suite)[0]
        except IndexError:
            return False
        # Convert to datetime and compare with day difference
        suite_date = datetime.strptime(date_str, "%Y-%m-%d")
        if suite_date > self.today - timedelta(days=self.day_delta):
            return True
        return False

    def restart(self, to_restart, dry_run, status=None):
        """Restart all suites in the list."""
        self.total_failed_tasks = 0
        for suite in self.failed_suites:
            if suite.suite not in to_restart:
                continue
            if status is not None and hasattr(status, "update"):
                status.update(f"Restarting {suite}")
            else:
                print(f"Restarting {suite}")
            suite.restart(dry_run)
            self.total_failed_tasks += suite.failed

    def __iter__(self):

        yield from self.failed_suites

    @property
    def failed(self):
        """Number of failed suites."""
        return len(self.failed_suites)


def run_command(command, dry_run=True):
    """
    Launch a subprocess command and return the output
    """
    if dry_run:
        # Short sleep in dry-run mode to simulate running a command
        sleep(0.1)
        return True
    return subprocess.run(command.split(), capture_output=True, text=True, check=False)


def ask_yn(message):
    """
    Ask a message with yes or no options
    Return True for y, False for n
    """
    rval = ""
    while rval.lower().strip() not in ["y", "n"]:
        rval = input(f"{message}? (y/n) ")
    return rval.lower().strip() == "y"


def rich_failure_table(nightlies):
    """Generate a rich formatted table of failed tasks."""
    table = Table(title="Failures")
    table.add_column("Suite")
    table.add_column("Date")
    table.add_column("Failed Tasks", justify="right", style="red")
    table.add_column("Successful Tasks", justify="right", style="green")

    for suite in nightlies:
        name, date = suite.suite.rsplit("_", 1)
        table.add_row(name, date, str(suite.failed), str(suite.succeeded))

    return table


# Ignore bad suggestion from pylint
# pylint: disable=consider-using-join


def text_failure_table(nightlies):
    """Generate plain text formatted table of failed suites."""
    result = "\nFound the following suites with errors:\n"
    for suite in nightlies:
        result += f"    * {suite}\n"
    return result


# pylint: enable=consider-using-join


def failure_table(nightlies, use_rich):
    """Table of failed suites."""
    if use_rich:
        return rich_failure_table(nightlies)
    return text_failure_table(nightlies)


def wait_for_restart(delay, use_rich):
    """Wait for suites to restart."""

    if use_rich:
        # Use a progress bar to count down
        for _ in track(
            range(delay),
            description="Waiting for suites to restart",
            transient=True,
        ):
            sleep(1)

    else:
        # Otherwise just report the time and sleep for the whole period
        print(
            f"\n{datetime.now().strftime('%H:%M:%S')} "
            f"Sleeping for {delay} seconds to allow suites to restart\n"
        )
        sleep(delay)


def process_arguments():
    """Process command line arguments."""

    parser = ArgumentParser(
        usage="%(prog)s [options] [suites]",
        description=__doc__,
        formatter_class=RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--dry-run", action="store_true", help="do not actually restart or retrigger"
    )

    parser.add_argument(
        "--run-all", action="store_true", help="run all tasks without prompting"
    )

    if RICH_SUPPORT:
        # If rich is available, allow it to be toggled off with a
        # command line option
        parser.add_argument(
            "--plain", action="store_true", help="run in plain text mode"
        )

    parser.add_argument(
        "--delay",
        type=int,
        default=120,
        metavar="S",
        help="restart delay in seconds (default: %(default)s)",
    )

    parser.add_argument(
        "--cylc-run",
        type=Path,
        default="~/cylc-run",
        metavar="DIR",
        help="location of cylc-run directory (default: %(default)s)",
    )

    parser.add_argument(
        "suites", type=str, nargs="*", help="names of suites being targeted"
    )

    args = parser.parse_args()

    args.cylc_run = args.cylc_run.expanduser()
    if not args.cylc_run.is_dir():
        parser.error("cylc_run must be a directory")

    if RICH_SUPPORT:
        args.use_rich = not args.plain
    else:
        args.use_rich = False

    return args


# pylint: disable=too-many-branches,too-many-statements


def main():
    """Main function."""

    args = process_arguments()

    if args.use_rich:
        console = Console()
        output = console.print
        ask = Confirm.ask
        cm = console.status
    else:
        output = print
        ask = ask_yn
        cm = contextlib.nullcontext

    nightlies = NightlyTesting(args.cylc_run, args.suites)

    with cm("Locating suites") as status:
        # Get a list of suites from the last day(weekend) to check for failed tasks
        nightlies.find_testsuites()

    if nightlies.failed == 0:
        output("No failed suites found")
        raise SystemExit(1)

    output(failure_table(nightlies, args.use_rich))

    if os.geteuid() != args.cylc_run.stat().st_uid:
        # The user does not own the cylc run directory, so they will
        # not be able to resetart/retrigger the suites.  Issue a
        # warning in dry-run mode and trigger and error if in live
        # mode
        if args.dry_run:
            output("warning: user does not own the suites")
        else:
            output("error: user does not own the suites")
            raise SystemExit(2)

    # Restart failed suites
    if not args.run_all:
        args.run_all = ask("Do you want to restart all failed suites")

    to_restart = []
    for suite in nightlies:
        if not args.run_all and not ask(f"Do you want to restart {suite}"):
            continue
        to_restart.append(str(suite))

    if not to_restart:
        output("No suites selected for restarting")
        raise SystemExit(1)

    with cm("Restarting suites") as status:
        nightlies.restart(to_restart, args.dry_run, status)
    output(f"Restarted {len(to_restart)} suites")

    wait_for_restart(args.delay, args.use_rich)

    if args.use_rich:
        # Rich progress bar which counts down as tasks are ticked off
        # and outputs a status message every time it starts working on
        # a new suite
        output(f"There are {nightlies.total_failed_tasks} tasks to retrigger")
        with Progress(transient=True) as progress:
            task = progress.add_task(
                "Restarting tasks", total=nightlies.total_failed_tasks
            )

            def advancer(current, total):
                """Closure which updates the status labels."""
                progress.update(task, description=f"Restarting {current}/{total}")
                progress.advance(task)

            for suite in nightlies:
                if suite.restarted:
                    progress.console.print(f"Working on suite {suite}")
                    progress.advance(task)
                    suite.retrigger(args.dry_run, advancer)

    else:
        # In plain text mode, use embedded print statements
        def advancer(current, total):
            """Closure which outputs a task update message."""
            output(f"\rTask {current}/{total}", end="", flush=True)

        for suite in nightlies:
            if suite.restarted:
                output(f"Working on suite {suite}")
                suite.retrigger(args.dry_run, advancer)
        output()

    output("Retriggering complete")


# pylint: enable=too-many-branches,too-many-statements


if __name__ == "__main__":

    main()
