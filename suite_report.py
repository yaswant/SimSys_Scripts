#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK
# *****************************COPYRIGHT*******************************
# (C) Crown copyright Met Office. All rights reserved.
# For further details please refer to the file COPYRIGHT.txt
# which you should have received as part of this distribution.
# *****************************COPYRIGHT*******************************
#
# NOTE
#
# This module is one of several for which the Master copy is in the
# UM repository. When making changes, please ensure the changes are
# made in the UM repository or they will be lost during the release
# process when the UM copy is copied over.
#

# The module docstring doubles as the command line description
"""
Process a cylc workflow and write a summary file using Trac wiki
markup.  If a local project mirror cannot be found, it will be
excluded from the final report.

The command can be triggered automatically at the end of a workflow by
adding the following to the cylc configuration:

    shutdown handler = "suite_report.py"

For interactive use, see the --help information.
"""

# FIXME: consider giving methods consistent names
# pylint: disable=too-many-lines

import io
import os
import re
import sqlite3
import sys
import traceback
import time
import subprocess
import json
from argparse import ArgumentParser, ArgumentTypeError, \
    RawDescriptionHelpFormatter
from collections import defaultdict
from pathlib import Path
from tempfile import TemporaryDirectory, mkstemp
from fcm_bdiff import get_branch_diff_filenames

try:
    import argcomplete
    COMPLETION = True
except ModuleNotFoundError:
    COMPLETION = False


TRAC_LOG_FILE = "trac.log"
DEFAULT_VERBOSITY = 3

PINK_FAIL_TEXT = "'''[[span(style=color: #FF00FF, pink failure )]]'''"
DESIRED_ORDER = [PINK_FAIL_TEXT, "failed", "succeeded"]

BACKGROUND_COLOURS = {
    "um": "#FFFFBF",
    "lfric_apps": "#E9D2FF",
    "jules": "#BFD0FF",
    "ukca": "#BFFFD1",
    "unknown": "#BFFFD1",
}

FCM = {
    "meto": "fcm",
    "ecmwf": "fcm",
    "nci": "fcm",
    "bom": "fcm",
    "uoe": "fcm",
    "niwa": "fcm",
    "kma": "fcm",
    "vm": "fcm",
    "jasmin": "fcm",
    "cehwl1": "fcm",
    "mss": "fcm",
    "ncas": "fcm",
    "psc": "fcm",
    "uoleeds": "fcm",
    "Unknown": "true",
}
RESOURCE_MONITORING_JOBS = {
    "meto": [
        "atmos-xc40_cce_um_fast_omp-seukv-4x9-noios-2t",
    ],
    "ecmwf": [],
    "nci": [],
    "bom": [],
    "uoe": [],
    "niwa": [],
    "kma": [],
    "vm": [],
    "jasmin": [],
    "cehwl1": [],
    "mss": [],
    "ncas": [],
    "psc": [],
    "uoleeds": [],
    "Unknown": [],
}
CYLC_REVIEW_URL = {
    "meto": "http://fcm1/cylc-review",
    "ecmwf": "Unavailable",
    "nci": "http://accessdev.nci.org.au/cylc-review",
    "bom": "http://scs-watchdog-dev/rose-bush",
    "uoe": "Unavailable",
    "niwa": "http://w-rose01.maui.niwa.co.nz/cylc-review",
    "kma": "Unavailable",
    "vm": "http://localhost/cylc-review",
    "jasmin": "Unavailable",
    "cehwl1": "Unavailable",
    "mss": "Unavailable",
    "ncas": "http://puma.nerc.ac.uk/cylc-review",
    "psc": "Unavailable",
    "uoleeds": "Unavailable",
    "Unknown": "Unavailable",
}
HIGHLIGHT_ROSE_ANA_FAILS = [
    "_vs_",
    "lrun_crun_atmos",
    "proc",
    "atmos_omp",
    "atmos_nruncrun",
    "atmos_thread",
    "-v-",
]
COMMON_GROUPS = {
    "meto": [
        "all",
        "nightly",
        "developer",
        "xc40",
        "ex1a",
        "spice",
        "xc40_nightly",
        "ex1a_nightly",
        "spice_nightly",
        "xc40_developer",
        "ex1a_developer",
        "spice_developer",
        "ukca",
        "recon",
        "jules",
        "xc40_ukca",
        "ex1a_ukca",
        "spice_ukca",
        "xc40_jules",
        "ex1a_jules",
        "spice_jules",
    ],
    "ecmwf": [],
    "nci": [],
    "bom": [],
    "uoe": [],
    "niwa": [],
    "kma": [],
    "vm": [],
    "jasmin": [],
    "cehwl1": [],
    "mss": [],
    "ncas": [],
    "psc": [],
    "Unknown": [],
}


def _run_command(command, ignore_fail=False):

    """Run a command and capture its output.

    A wrapper function that runs a command and returns the output in a
    usable form.

    Args:
        command (:obj:`list` of :obj:`str`): elements of the command
            being run
        ignore_fail (:obj:`bool`, optional): whether to raise an
            exception on a non-zero return code

    Returns:
        tuple: the exit code as an integer, standard output and
            stadnard error as lists of lines.

    Raises:
        IOError: if ignore_fail is set and the command returns non-zero
    """

    with subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            encoding="utf-8",
    ) as pobj:
        pobj.wait()
        retcode, stdout, stderr = (
            pobj.returncode,
            pobj.stdout.read(),
            pobj.stderr.read(),
        )
    if retcode != 0 and not ignore_fail:
        # FIXME: replace with a custom exception
        print(f"[ERROR] running {command}")
        print(f"[INFO] RC: {retcode}")
        print(f"[INFO] Stdout: {stdout}")
        print(f"[INFO] Stderr: {stderr}")
        raise IOError("run_command")
    # Reformat stdout into a list
    stdout = "".join(stdout)
    stdout = stdout.split("\n")

    return retcode, stdout, stderr


def _remove_quotes(string):

    """Remove double and single quotes from a string.

    Args:
        string (str): input string or None

    Returns:
        str: input value without the quotes or None if input was None
    """
    if string is not None:
        string = re.sub(r'"', r"", string)
        string = re.sub(r"'", r"", string)
    return string


def _dict_merge(main_dict, addon_dict, force=False):

    """Selectively combine two dictionaries to create a new one.

    Create a copy of a dictionary that contains items from a second
    that are not in the first.  If any entry in the addon dictionary
    is itself a dictionary, its contents are duplicated by
    _dict_merge.

    Where an item is in both dictionaries, the value in main has
    priority unless force is True.  Where a key does not exist in main
    and the value in addon is None, a new key will not be set in main
    unless force is True.  These behaviours differentiate it from
    dict.update()

    Args:
        main_dict (dict): dictionary of base values
        addon_dict (dict): dictionary to merge into the main one
        force (:obj:`bool`, optional): always copy the value from
            addon, even it is None.  Default is False, which implies
            that keys with None values will not be set.

    Returns:
        dict: copy of the main dictionary with addon items merged in
    """
    merged_dict = main_dict.copy()
    for key, value in addon_dict.items():
        if isinstance(value, dict):
            if key not in merged_dict:
                merged_dict[key] = {}
            # FIXME: recursive call should pass the caller's force setting?
            merged_dict[key] = _dict_merge(merged_dict[key], value)
        else:
            # Switch to Force main to take whatever addon has
            # No matching key in main - take whatever addon has including None
            # Or
            # Override main with contents of addon
            if force or key not in merged_dict or value is not None:
                merged_dict[key] = value
    return merged_dict


def _get_current_head_revision(mirror_url, fcm_exec):

    """Get the head revision using a local mirror URL.

    Args:
        mirror_url (str): location of the local mirror repository
        fcm_exec (str): path to the FCM command

    Returns:
        str: the revision number of the head of the project trunk or
            or an empty string if no revision was found.
    """
    revision = ""
    _, stdout, _ = _run_command([fcm_exec, "branch-info", mirror_url])
    find_last_changed_rev = re.compile(r"Last Changed Rev:\s*(\d+)")
    for line in stdout:
        result = find_last_changed_rev.search(line)
        if result:
            revision = str(result.group(1))
            break
    return revision


def _url_to_trac_link(url):
    """Convert a URL to a Trac code link.

    Takes a URL amd edits text to resemble a Trac link for code on the
    SRS.

    Args:
        url (str): a subversion URL

    Returns:
        str: a Trac link form of URL or None if 'svn' was absent from
            the url
    """
    if re.search(r"/svn/", url):
        link_2_url = re.sub(r"svn", r"trac", url)
        elements = link_2_url.split("/")
        elements.insert(elements.index("trac") + 2, "browser")
        link_2_url = "/".join(elements)
        link_2_url = re.sub(r"@", r"?rev=", link_2_url)
    else:
        link_2_url = None
    return link_2_url


class CylcVersion:
    """Class to abstract cylc configuration.

    An interface to cylc workflows which can be used with both cylc 7
    and cylc 8.

    Args:
        cylc_path (str): root directory of an installed cylc workflow
    """

    PROCESSED_SUITE_RC_CYLC7 = "suite.rc.processed"
    ROSE_SUITE_RUN_CONF_CYLC7 = "rose-suite-run.conf"
    ROSE_SUITE_RUN_CONF_CYLC8 = "-rose-suite.conf"
    SUITE_DB_FILENAME_CYLC7 = "cylc-suite.db"
    SUITE_DB_FILENAME_CYLC8 = "db"

    prefix_mosrs = "https://code.metoffice.gov.uk/svn/"
    prefix_svn = "svn://fcm1/"

    def __init__(self, cylc_path):

        # Default to cylc 8
        self.cylc_path = Path(cylc_path).resolve()
        self.cylc_log = self.cylc_path / "log"
        self.cylc_config = self.cylc_log / "config"
        self.version = 8

        if not self.cylc_config.is_dir():
            self.version = 7
            self.cylc_config = self.cylc_log

        if not self.cylc_config.exists():
            raise ValueError("Not a valid cylc run directory: " + repr(cylc_path))

        # Separate the suite directory and try to get the current
        # username
        home, self.workflow_name = str(self.cylc_path).split("cylc-run/")
        self.workflow_owner = os.environ.get(
            "CYLC_SUITE_OWNER", os.path.basename(home.rstrip("/"))
        )

    @property
    def suiterc_processed_path(self):
        """Path to the processed suite file."""

        if self.version == 7:
            path = self.cylc_path / self.PROCESSED_SUITE_RC_CYLC7

        else:
            path = None
            for filename in os.listdir(self.cylc_config):
                path = self.cylc_config / filename
                if path.is_file() and self.ROSE_SUITE_RUN_CONF_CYLC8 in str(filename):
                    break

        # Check file exist and force an exception if not
        path = path.absolute()
        path.stat()

        return path

    @property
    def rose_suite_run_path(self):
        """Path to the Rose suite run file."""

        if self.version == 7:
            path = self.cylc_log / self.ROSE_SUITE_RUN_CONF_CYLC7

        else:
            # Catch the first file in the directory
            path = None
            for path in self.cylc_config.glob(f"*{self.ROSE_SUITE_RUN_CONF_CYLC8}"):
                break

            if path is None:
                raise IOError(2, "Suite config file not found",
                              str(self.cylc_config))

        # Check file exist and force an exception if not
        path = path.absolute()
        path.stat()

        return path

    @property
    def database_path(self):
        """Path to the cylc database."""

        if self.version == 7:
            path = self.cylc_path / self.SUITE_DB_FILENAME_CYLC7

        else:
            path = self.cylc_log / self.SUITE_DB_FILENAME_CYLC8

        # Check file exist and force an exception if not
        path = path.absolute()
        path.stat()

        return path

    def _cylc7_project_details(self):
        """Get cylc 7 project details.

        Locates the .version files and parse them to obtain a
        dictionary of projects and a count of uncommitted changes.

        Returns:
            tuple: a dictionary of projects and an integer containing the
                number of uncommitted changes
        """

        projects = {}
        uncommitted_changes = 0

        find_proj_name = re.compile(r"/(\w+)-\d+.version")
        version_files = []
        version_files = (self.cylc_path / "log ").glob("/*.version")

        for vfile in version_files:
            if "rose-suite-run.version" in vfile.name:
                continue
            result = find_proj_name.search(vfile)
            if result:
                project = result.group(1).upper()
                projects[project] = {}
                url, revision, wc_changes = self._cylc7_parse_details(vfile)
                projects[project]["last changed rev"] = revision
                projects[project]["working copy changes"] = wc_changes
                projects[project]["version file"] = vfile.name
                if wc_changes:
                    uncommitted_changes += 1
                if url is not None:
                    if revision is not None:
                        ending = "@" + revision
                    else:
                        ending = ""
                    projects[project]["repo loc"] = url + ending

        return projects, uncommitted_changes

    @staticmethod
    def _cylc7_parse_details(vfile):
        """Parse a cylc 7 version file.

        Parses a versions file to extract the url and revision for the
        branches behind any working copies, plus any uncommitted
        changes.

        Args:
            vfile (Path): location of the .version file

        Returns:

            tuple: a string containing a URL, a string containg the
                revions, and boolean indicating whether there are
                changes to the working copy.
        """

        url = None
        revision = None
        working_copy_changes = False
        find_svn_status = re.compile(r"SVN STATUS", re.IGNORECASE)
        find_url = re.compile(r"URL:\s*")
        find_last_changed_rev = re.compile(r"Last Changed Rev:\s*")

        with open(vfile, encoding="utf-8") as source:
            for line in source:
                if find_svn_status.search(line):
                    working_copy_changes = True
                if find_url.match(line):
                    url = find_url.sub(r"", line).rstrip()
                if find_last_changed_rev.match(line):
                    revision = find_last_changed_rev.sub(r"", line).rstrip()

        return url, revision, working_copy_changes

    def _cylc8_project_details(self):
        """Get cylc 8 project details.

        Parse the cylc vcs.json file to obtain a dictionary of
        projects and a count of uncommitted changes.

        Returns:
            tuple: a dictionary of projects and an integer containing the
                number of uncommitted changes
        """

        projects = {}
        uncommitted_changes = 0

        vcs_path = self.cylc_log / "version" / "vcs.json"
        with open(vcs_path, encoding="utf-8") as vcs_file:
            vcs_data = json.load(vcs_file)

        if (
            "url" not in vcs_data
            or "revision" not in vcs_data
            or "status" not in vcs_data
        ):
            # Allow the caller to hand the situation where one of the
            # required keys cannot be found
            raise KeyError(f"{vcs_file} lacks url, revision, or status")

        if vcs_data["url"] is not None:
            ending = "" if vcs_data["revision"] is None else "@" + vcs_data["revision"]
            project = vcs_data["url"]

            if project.startswith(self.prefix_mosrs):
                project = project[len(self.prefix_mosrs):]
            if project.startswith(self.prefix_svn):
                project = project[len(self.prefix_svn):]
            project = re.split("[/.]", project)[0].upper()
            projects[project] = {}

            # Use the version control url as the project source
            # This url isn't necessarily to top of the working copy so split
            # the url around "branches" or "trunk" to ensure the correct url
            url = vcs_data["url"]
            splitter = "branches" if "branches" in url else "trunk"

            start_url, end_url = url.split(f"/{splitter}/", 1)
            start_url += f"/{splitter}/"
            end_url = end_url.split("/")
            if splitter == "branches":
                # For branches, format is
                # "/[dev|test]/<username>/<branch-name>"
                end_url = f"{end_url[0]}/{end_url[1]}/{end_url[2]}"
            else:
                # For trunk, format is just "/trunk/"
                end_url = ""
            projects[project]["repo loc"] = start_url + end_url + ending

        for item in vcs_data["status"]:
            if not item.startswith("?") and len(item) > 0:
                uncommitted_changes += 1

        return projects, uncommitted_changes

    def project_details(self):
        """Get project details.

        Obtain a dictionary of projects and a count of uncommitted
        changes for a workflow, independent of the version of cylc
        being used.

        Returns:
            tuple: a dictionary of projects and an integer containing the
                number of uncommitted changes
        """

        if self.version == 7:
            return self._cylc7_project_details()

        return self._cylc8_project_details()

    def task_states(self):
        """Get task states from the cylc database.

        Uses sqlite3 to query the cylc database for information about
        the states of the tasks in the workflow.

        Returns:
            dict: a dictionary of task states where they keys are the
                task names and the values are states as strings.
        """

        database = sqlite3.connect(self.database_path)
        cursor = database.cursor()
        cursor.execute("select name, status from task_states;")
        data = {}
        for row in cursor:
            data[row[0]] = row[1]
        database.close()
        return data

    @classmethod
    def default_cylc_path(cls):
        """Default cylc directory path.

        Uses environment variables to guess at the path to the cylc
        workflow directory.

        This class method is intended to be used to set a sensible
        default for use with ArgumentParser.

        Returns:
            str: the path of the workflow or None if the path cannot
                be obtained from the environment
        """

        # Try cylc 8 first
        path = os.environ.get("CYLC_WORKFLOW_RUN_DIR")

        if path is None:
            # Next try cylc 7
            path = os.environ.get("CYLC_SUITE_RUN_DIR")

        if path is not None:
            # Convert valid paths to pathlibs
            path = Path(path)

        # Return the path or None if environment is not set
        return path

    @property
    def suite_scheduler_log_path(self):

        """Path to the suite scheduler log."""

        if self.version == 7:
            path = self.cylc_log / "suite"
        else:
            path = self.cylc_log / "scheduler" / "log"

        return path.absolute()


class Project:

    """Class coontains details of each project.

    Class which encapsulates information about a specific project and
    provides a dicionary-like interface to its parameters.  A project
    in this context is an fcm repository.

    Args:
        name (str): project name
        params (dict): various parameters associated with the project
        owner (str): the parent JobSources container that owns the
            instance.  This is used when callbacks to the owning
            class are needed

    """

    fcm_exec = None

    def __init__(self, name, params, owner):

        self.name = name
        self._raw_params = params
        self._owner = owner
        self.params = {}

        self["tested source"] = _remove_quotes(params["tested source"])

        target = params.get("repo loc", self["tested source"])
        self["repo loc"] = self.convert_to_srs(target)
        self["repo mirror"] = self.convert_to_mirror(self["repo loc"])

        # Check validity of the mirror
        self.valid = self.check_repository(self["repo mirror"])
        if not self.valid:
            # Skip further actions
            return

        self["parent mirror"] = self.get_parent(self["repo mirror"])
        self["parent loc"] = self.convert_to_srs(self["parent mirror"])

        self.get_revisions()
        self.get_links()

        self["human repo loc"] = self.convert_to_keyword(
            self["repo loc"]
        )
        self["human parent"] = self.convert_to_keyword(
            self["parent loc"]
        )

        self["ticket no"] = self.get_ticket_number(
            self["repo mirror"]
        )

        self["bdiff_files"] = self.get_altered_files_list(
            self["repo mirror"]
        )

    def get_revisions(self):

        """Get the revision of the project.

        Use fcm to get information about the head revision of the
        project and save this information in the instance.
        """

        for location in ("repo", "parent"):
            url = self[location + " loc"]
            mirror_url = self[location + " mirror"]
            if url is None or mirror_url is None:
                continue
            if ":" in url and "@" not in url:
                revision = _get_current_head_revision(mirror_url, self.fcm_exec)
                self[location + " loc"] = url + "@" + revision
                self[location + " mirror"] = (
                    mirror_url + "@" + revision
                )

    def get_links(self):

        """Get the repository links associated with the project.

        Get information about the repository SNV paths and store them
        in the instance.
        """

        # If those attempts to generate links didn't work, try the hope
        # and guess approach.
        self["repo link"] = self.convert_to_link(self["repo loc"])
        if self["repo link"] is None:
            self["repo link"] = self.convert_link_from_layout(
                    self["repo link"], self["repo mirror"]
                )

        self["parent link"] = self.convert_to_link(self["parent loc"])
        if self["parent link"] is None:
            self["parent link"] = self.convert_link_from_layout(
                    self["parent loc"],
                    self["parent mirror"],
                )
        # Final attempt to ensure the links have revision numbers and not
        # keywords which aren't evaluated in the browser.
        if self["repo link"] is not None and re.search(
                r"rev=[a-zA-z]", self["repo link"]):
            revision = self.get_revision_from_layout(
                    self["repo mirror"]
            )
            self["repo link"] = re.sub(
                    r"rev=[a-zA-z0-9.]+",
                    "rev=" + revision,
                    self["repo link"],
                )

    def get(self, key, default=None):

        """Get a value for a keyword or return a default.

        This emulates the behaviour of a dictionary instance by
        fetching the value associated with a key or a default value.

        Args:
            key (str): the name of the target resource
            default ((:obj:`object`, optional): a value to return if
                the keyword does not exist.  Defaults to None.
        """

        return self.params.get(key, default)

    def __setitem__(self, key, value):

        self.params[key] = value

    def __getitem__(self, key):

        return self.params[key]

    def __contains__(self, key):

        return key in self.params

    def get_parent(self, mirror_url):

        """Get the branch parent of the project.

        Uses fcm to locate the parent branch of the project, if any.

        Args:
            mirror_url (str): the location of the repository.

        Returns:
            str: the URL of parent branch or None if there is no parent
        """

        parent = None
        stdout = ""
        command = [self.fcm_exec, "branch-info", mirror_url]
        _, stdout, _ = _run_command(command, ignore_fail=True)
        find_branch_parent = re.compile(r"Branch Parent:\s*(.*)")
        for line in stdout:
            result = find_branch_parent.search(line)
            if result:
                parent = result.group(1).rstrip()
        return parent

    def check_repository(self, url):

        """Whether a repository is accessible.

        Args:
            url (str): location of either an SRS or mirror repository

        Returns:
            bool: True if the repository exists, False otherwise.
        """
        retcode = 0
        command = [self.fcm_exec, "info", url]
        retcode, _, _ = _run_command(command, ignore_fail=True)
        if retcode == 0:
            return True
        return False

    def convert_to_srs(self, url):
        """Convert a URL to an SRS URL.

        Checks the supplied URL against those in the owning instance
        and returns an SRS URL.  This is the inverse of
        convert_to_mirror().

        Args:
            url (str): location of a repository which might be an
                internal mirror

        Returns:
            str: the full SRS URL when a match is found.  Otherwise
                the original URL is returned unchanged
        """
        if url is None:
            return None
        srs_url = url
        for proj, proj_url in self._owner.urls.items():
            # Only check for keywords which correspond to mirror or SRS format
            if re.search(r".x(|m)$", proj):
                if re.search(proj_url, url):
                    # checking given url against urls in the owning class
                    shared_project = re.sub(r"m$", r"", proj)
                    if shared_project in self._owner.urls:
                        mirror_url = proj_url
                        shared_url = self._owner.urls[shared_project]
                        srs_url = re.sub(mirror_url, shared_url, url, count=1)
                        break
                elif re.search("fcm:" + proj + r"[^m]", url):
                    # Looking for an fcm: shorthand notation based on keyword.
                    shared_project = re.sub(r"m$", r"", proj)
                    if shared_project in self._owner.urls:
                        # if the fcm keyword ends in '_tr' it's on the trunk
                        if re.match(r"fcm:" + proj + r"_tr", url):
                            srs_url = re.sub(
                                r"fcm:" + proj + r"_tr",
                                self._owner.urls[shared_project] + r"/trunk",
                                url,
                                count=1,
                            )
                        # if the fcm keyword ends in '_br' it's from branches
                        elif re.match(r"fcm:" + proj + r"_br", url):
                            srs_url = re.sub(
                                r"fcm:" + proj + r"_br",
                                self._owner.urls[shared_project] + r"/branches",
                                url,
                                count=1,
                            )
                        # maintain keyword style, but convert to srs.
                        else:
                            srs_url = re.sub(proj, shared_project, url,
                                             count=1)
                        break
        return srs_url

    def convert_to_mirror(self, url):

        """Convert an SRS URL to a mirror URL.

        Checks the supplied SRS URL against those in the owning
        instance and returns a mirror URL.  This is the inverse of
        convert_to_srs().

        This assumes that mirror projects end in 'm'.

        Args:
            url (str): location of a repository which might be an
                SRS URL.

        Returns:
            str: the mirror URL when a match is found.  Otherwise
                the original URL is returned unchanged
        """
        if url is None:
            return None
        mirror_url = url
        for proj, proj_url in self._owner.urls.items():
            # checking given url against urls in the owning class
            if proj_url in url:
                new_proj = proj + "m"
                if new_proj in self._owner.urls:
                    old_proj_url = proj_url
                    new_proj_url = self._owner.urls[new_proj]
                    mirror_url = re.sub(
                        old_proj_url, new_proj_url, url, count=1
                    )
                    break
            # checking given url against keywords in the owning class
            elif proj in url:
                new_proj = proj + "m"
                if new_proj in self._owner.urls:
                    mirror_url = re.sub(proj, new_proj, url, count=1)
                    break
        return mirror_url

    def convert_to_keyword(self, url):
        """Convert a URL to fcm keyword-based version.

        Checks the supplied URL against those in the owning instance
        and converts it into an fcm keyword-style name.

        Args:
            url (str): location of a repository

        Returns:
            str: a keyword location or None if no keyword is defined
        """
        if url is None:
            return None
        keyword_url = None
        for proj, proj_url in self._owner.urls.items():
            if proj_url in url:
                new_proj_url = f"fcm:{proj}"
                keyword_url = re.sub(proj_url, new_proj_url, url, count=1)
                keyword_url = re.sub(r"/trunk", "_tr", keyword_url, count=1)
                keyword_url = re.sub(r"/branches", "_br", keyword_url, count=1)
                break
            if "fcm:" + proj in url:
                keyword_url = url
                break
        return keyword_url

    def convert_to_link(self, url):

        """Convert a URL into a Trac wiki format link.

        Converts a subversion URL into a Trac link.

        Args:
            url (str): location of a repository

        Returns:
            str: a Trac link or None if the URL cannot be resolved
        """
        link = None
        if url is not None:
            # Look for a matching part of the URL in the list of projects
            for _, svn in self._owner.urls.items():
                if re.search(svn, url):
                    link = _url_to_trac_link(url)
                    break
        return link

    def convert_link_from_layout(self, url, mirror_url):

        """Convert URL to a link location using a heuristic.

        Converts a URL to a link using assumptions based on working
        practices at the Met Office.

        Args:
            url (str): location of the repository
            mirror_url (str): location of a mirror of the repository

        Returns:
            str: a link or None if the project could not be found.
        """
        link = None
        if url is None or mirror_url is None or re.search(r"^file:/", url):
            return None
        _, stdout, _ = _run_command([self.fcm_exec, "loc-layout", mirror_url])
        path = None
        root = None
        lproject = None
        revision = None
        find_path = re.compile(r"^path:\s*")
        find_root = re.compile(r"^root:\s*")
        find_project = re.compile(r"^project:\s*")
        find_peg_rev = re.compile(r"^peg_rev:\s*")
        for line in stdout:
            if find_path.match(line):
                path = find_path.sub(r"", line)
                continue
            if find_root.match(line):
                root = find_root.sub(r"", line)
                continue
            if find_project.match(line):
                lproject = find_project.sub(r"", line)
                continue
            if find_peg_rev.match(line):
                revision = find_peg_rev.sub(r"", line)
        if root is not None and lproject is not None and path is not None:
            # Convert to a trac url.
            if re.search(r"/svn/", url):
                url = re.sub(r"svn", r"trac", url)
                elements = url.split("/")
                elements.insert(elements.index("trac") + 2, "browser")
                url = "/".join(elements)
                if revision is not None:
                    link = url + f"?rev={revision}"
                else:
                    link = url
        return link

    def get_revision_from_layout(self, mirror_url):
        """Get a revision number from the mirror repository.

        Gets a revision number using a url to the mirror repository.

        Args:
            mirror_url (str): location of a mirror of the repository

        Returns:
            str: the revision number or None if not found
        """
        if mirror_url is None:
            return None
        _, stdout, _ = _run_command([self.fcm_exec, "loc-layout", mirror_url])
        revision = None
        find_peg_rev = re.compile(r"^peg_rev:\s*")
        for line in stdout:
            if find_peg_rev.match(line):
                revision = find_peg_rev.sub(r"", line)
                break
        return revision

    def get_ticket_number(self, mirror_url):

        """Get ticket number from the Trac log.

        Uses the assumption that fcm log messages follow the working
        practices and start with a ticket number to match the URL to a
        Trac ticket.

        Args:
            mirror_url (str): location of a mirror of the repository

        Returns:
            str: the trac ticket number or None if not found
        """
        ticket_number = None
        if re.search("/trunk[/@$]", mirror_url) or re.search(
            r"[fs][cv][mn]:\w+(.xm|.x|)_tr[/@$]", mirror_url
        ):
            return ticket_number
        _, stdout, _ = _run_command([self.fcm_exec, "log", "-l", "1", mirror_url])
        for line in stdout:
            result = re.search(r"^\s*(#\d+)", line)
            if result:
                ticket_number = result.group(1)
        return ticket_number

    @staticmethod
    def get_altered_files_list(mirror_loc):

        """Get a list of files altered by the branch.

        Uses fcm_bdiff.get_branch_diff_filenames to get a list of
        files edited on a branch.  Entries that do not contain a file
        extension are assumed to directories and are removed from the
        list.

        Args:
            mirror_url (str): location of a mirror of the repository

        Returns:
            list: a list of strings pointing to files
        """
        for attempt in range(5):
            # pylint: disable=broad-exception-caught

            try:
                # Get a list of altered files from the fcm mirror url
                bdiff_files = get_branch_diff_filenames(
                    mirror_loc, path_override=""
                )
                break
            except Exception as err:
                print(err)
                if attempt == 4:
                    print(
                        "Cant get list of alterered files - returning "
                        "empty list."
                    )
                    bdiff_files = []
                    break

            # pylint: enable=broad-exception-caught

        # If '.' is in the files list remove it
        try:
            bdiff_files.remove(".")
        except ValueError:
            pass

        # Remove any item that is not a file - decide based on whether the item
        # has a file extension
        for item in bdiff_files:
            # The last part of the file path
            file_name = item.split("/")[-1]
            # If the final part doesn't have a file extension remove this item
            # as it is a directory
            if "." not in file_name:
                bdiff_files.remove(item)

        return bdiff_files


class JobSources:

    """Class containing information about all the job sources.

    Class which ncapsulates information about projects and provides
    simple set of item-like accessor methods to query them.
    """

    def __init__(self):

        self.job_sources = {}
        self.urls = {}
        self._primary_project = None
        self.projects = {}

    def __iadd__(self, extras):

        self.job_sources = _dict_merge(self.job_sources, extras)
        return self

    def __contains__(self, key):

        return key in self.job_sources

    def __getitem__(self, key):

        return self.projects[key]

    def __iter__(self):

        yield from self.projects.items()

    def source_items(self):

        """Iterate over project and parameter items.

        Yields:
            tuple: a string containing the project name and a Project
                instance
        """

        yield from self.job_sources.items()

    def add_urls(self, urls):

        """Add a URL dictionary to the instance.

        Args:
            urls (dict): a dictionary of URLs
        """

        self.urls = urls.copy()

    def setup(self):

        """Setup a series of Project instances."""

        invalid = []
        for project, params in self.source_items():
            item = Project(project, params, self)
            if item.valid:
                self.projects[project] = item
            else:
                invalid.append(project)

        # Remove invalid sources
        for project in invalid:
            del self.job_sources[project]

    @property
    def primary_project(self):

        """The primary project based on the available sources.

        The name of the primary project based on the heirarchical
        links between the various different repositories.

        Returns:
            str: the name of the primary project
        """

        if self._primary_project is None:
            # Set the first time the value is requested
            if "LFRIC_APPS" in self.job_sources.keys():
                self._primary_project = "LFRIC_APPS"
            elif "UM" in self.job_sources.keys():
                self._primary_project = "UM"
            elif "JULES" in self.job_sources.keys():
                self._primary_project = "JULES"
            elif "UKCA" in self.job_sources.keys():
                self._primary_project = "UKCA"
            else:
                self._primary_project = "UNKNOWN"

        return self._primary_project


class TracFormatter:

    """Mixin which formats items for use with the Trac wiki.

    A mixin which contains a set of methods which can be used to send
    Trac wiki formatted items to an output stream.  These can be used
    with an io.StringIO instance to buffer the output rather than
    writing out immediately.

    This class makes use of the ability to send values to generators.
    Note that the first send to a new generator always needs to be
    None to run the generator code to the first yield point.
    """

    @staticmethod
    def format_trac_header(title=None, output=sys.stdout):

        """Create a tabulated report header.

        Uses a generator which allows header fields - a keyword and a
        value - to be added to a table that does have column headers.
        This is intended to be used to generate the header section of
        a the suite report.

        Args:
            title (:obj:`str`, optional): an optional section heading
                to add to the output stream before the tabular fields.
                Defaults to None which implies no section header is
                required
            output (:obj:`io`, optional): an IO stream of some sort.
                Defaults to sys.stdout
        """

        if title is not None:
            print(f" = {title} = \n", file=output)

        while True:
            row = yield

            if row is None or len(row) != 2 or not row[1]:
                # Do nothing if value is not set
                continue

            # Print to the output stream
            print(f" || {row[0]}: || {row[1]} || ", file=output)

    @staticmethod
    def format_trac_text(text_list, link, bold=False):

        """Generate a Trac format text element.

        Takes a list of text items, finds the first non-None value,
        and converts it into a Trac wiki-formatted string.

        Args:
            text_list (:obj:`list` of :obj:`str`): a list of values
                which should be strings which which may be None.  The
                first non-None value is assumed to be the required
                string
            link (str): a string containing a URL or None
            bold (:obj:`bool`, optional): whether to add Trac bold
                codes around the text item.  Defaults to False which
                implies that bold should not be applied

        Returns:
            str: a formatted string containing the first text string
                or an empty string if no non-None values can be found
                in text_list
        """
        # Get the first non-None item in the text list, if any
        text = next((i for i in text_list if i is not None), None)
        highlight = "'''" if bold else ""
        if text is not None and link is not None:
            element = f" {highlight}[{link} {text}]{highlight} "
        elif text is not None:
            # Insert an escape character into svn URLs if necessary
            text = re.sub(r"(?<!!)(svn://)", r"!\1", text)
            element = f" {highlight}{text}{highlight} "
        else:
            element = ""
        return element

    @staticmethod
    def format_trac_table(columns, title=None, preamble=None, output=sys.stdout):

        """Create a generic table.

        Uses a generator which creates a table framework and allows
        individual rows to be added with subsequent send calls.

        Args:
            columns (:obj:`list` of :obj:`str`): a set of values used
                to head up each column.  This is also used to
                determine how many values can be passed in to each row
                without an error occurring
            title (:obj:`str`, optional): an optional table header.
                Defaults to None which implies no header is needed.
            output (:obj:`io`, optional): an IO stream of some sort.
                Defaults to sys.stdout.

        Raises:
            IndexError: raised when the number of items in a row does
                not match the number of columns

        """

        if isinstance(columns, (str, int, float)):
            # Ensure that columns are a sequence
            columns = [columns]

        column_count = len(columns)

        if title:
            # Add a title, if provided
            print(f"'''{title}'''", file=output)

        if preamble:
            # Add something that isn't a set of column headers
            print("".join([f" || '''{'' if i is None else i}'''" for i in preamble])
                  + " ||", file=output)

        # Add the column headers
        print("".join([f" || '''{i}'''" for i in columns]) + " ||", file=output)

        while True:
            # Add a row every time one is provided
            row = yield
            if not isinstance(row, (list, tuple)):
                row = [row]
            else:
                row = list(row)

            row_length = len(row)

            if row_length > column_count:
                # Complain if row is too long
                raise IndexError(f"row is too long for table: {repr(row)}")

            if row_length != column_count:
                # Pad the row to match the number of columns
                row += [""] * (column_count - row_length)

            # Add the row, replacing None values with empty strings
            print("".join([f" || {'' if i is None else i}" for i in row]) + " ||",
                  file=output)


# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-public-methods

class SuiteReport(TracFormatter):

    """Class which produces a suite summary report.

    Class to to hold data and methods required to produce a suite report
    from a rose-stem suite output.

    Args:
        suite_path (Path): path to the suite output directory

        log_path ((:obj:`Path`, optional): path to the output
            directory which should contain the trac.log file.
            Defaults to None which implies that suite_path should be
            used.

        verbosity ((:obj:`int`, optional): dictates what tasks are
            omitted from the log.  Defaults to the value of
            DEFAULT_VERBOSITY.

        sort_by_name ((:obj:`bool`, optional): force sorting by task
            name over status when generating the task table in the
            report.  Defaults to False which implies tasks should be
            sorted by status and then name.
    """

    def __init__(
        self,
        suite_path,
        log_path=None,
        verbosity=DEFAULT_VERBOSITY,
        sort_by_name=False,
    ):
        self.suite_path = Path(suite_path).absolute()

        self.cylc_version = CylcVersion(self.suite_path)
        self.cylc = str(self.cylc_version.version)
        self.fcm = None
        self.rose = None

        self.host_xcs = False
        self.trustzone = os.environ.get("TRUSTZONE", None)

        self.log_path = Path(log_path) if log_path is not None else self.suite_path
        self.sort_by_name = sort_by_name
        self.verbosity = verbosity
        self.creation_time = time.strftime("%Y/%m/%d %X")
        self.uncommitted_changes = 0

        # Create a managed temporary directory.  Don't use a with
        # block because this needs to persist for the lifetime of the
        # instance
        # pylint: disable=consider-using-with
        self.tmpdir = TemporaryDirectory()
        # pylint: enable=consider-using-with

        self.site = "Unknown"
        self.rose_orig_host = None
        self.groups = []
        self.job_sources = JobSources()
        self.primary_project = ""
        self.projects = {}
        self.status_counts = defaultdict(int)
        self.status_counts["failed"] = 0

        try:
            # Resolve "runN" soft link - Required for Cylc8
            # cylc-review path
            link_target = self.suite_path.readlink()

            if link_target.is_absolute():
                # Link target is already an absolute path
                suitename = link_target
            else:
                # Link target is relative to its parent
                suitename = self.suite_path.parent / link_target

        except OSError:
            suitename = self.suite_path

        suite_dir, self.suitename = str(suitename).split("cylc-run/")
        # Default to userID from suite path unless CYLC_SUITE_OWNER is
        # present
        self.suite_owner = os.environ.get(
            "CYLC_SUITE_OWNER",
            os.path.basename(suite_dir.rstrip("/"))
        )

        self.parse_rose_suite_run()
        self.initialise_projects()
        self.parse_processed_config_file()
        details, self.uncommited_changes = self.cylc_version.project_details()
        self.job_sources += details
        self.job_sources.add_urls(self.projects)

        # Work out which project this suite is run as - heirarchical structure
        # with lfric_apps at the top, then UM, then the rest
        self.primary_project = self.job_sources.primary_project

        self.groups = [_remove_quotes(group) for group in self.groups]

        fcm_exec = FCM[self.site]
        Project.fcm_exec = fcm_exec

        self.job_sources.setup()

        # Check to see if ALL the groups being run fall into the
        # "common groups" category. This is used to control automatic
        # hiding of successful tasks later.
        if self.site == "meto" and "all" in self.groups:
            self.only_common_groups = True
        else:
            self.only_common_groups = all(
                    group.strip() in COMMON_GROUPS[self.site]
                    for group in self.groups
            )

    def parse_processed_config_file(self):

        """Parse the suite.rc.processed file.

        Extracts all projects present that begin with a SOURCE_ and
        allows them to override any SOURCE_<project>_<extension>
        entries.  Values are are stored in the job_sources dictionary
        attribute in the form {<project> : <URL>,...}

        This also extracts the host machine rose was launched on and
        stores the value in rose_orig_host attribute and updates the
        multi_branches attribute where there are multiple sources.
        """

        rose_orig_host = "Unknown rose_orig_host"

        srp_file = self.cylc_version.suiterc_processed_path

        find_orig_host = re.compile(r"ROSE_ORIG_HOST\s*=\s*(.*)")
        # in pattern below, need to include "_REV" after the project name and
        # before the " *=" and then exclude lines with "_REV" later as
        # otherwise the search will identify PROJ_REV as a unique project
        # name. The other option would be to have an alternate 3rd group match
        # of "_.*?" but that would exclude any future project names that might
        # have an underscore in them.
        find_sources = re.compile(
            r"\s*(?:HOST_SOURCE_|SOURCE_)(.*?)(|_BASE|_MIRROR|_REV)\s*=\s*(.*)"
        )
        sources = {}
        multiple_branches = {}

        with open(srp_file, encoding="utf-8") as source:
            for line in source:
                # check for ROSE_ORIG_HOST
                result = find_orig_host.search(line)
                if result:
                    rose_orig_host = result.group(1).rstrip()
                # check for SOURCE_.*
                result = find_sources.match(line)
                # Discard the ones which were SOURCE_PROJ_REV
                if result and result.group(2) != "_REV":
                    # Allow SOURCE_PROJ to override any existing entries
                    # Otherwise only add new entries
                    if result.group(1) not in sources or result.group(2) == "":
                        sources[result.group(1)] = {}
                        if " " in result.group(3):
                            multiple_branches[(result.group(1))] = result.group(3)
                            sources[result.group(1)][
                                "tested source"
                            ] = result.group(3).split()[0]
                        else:
                            sources[result.group(1)][
                                "tested source"
                            ] = result.group(3)

        self.rose_orig_host = rose_orig_host
        self.job_sources += sources
        self.multi_branches = multiple_branches

    @staticmethod
    def unpack_suite_value(value, remove_quotes=True, split_on_comma=False):

        """Unpack a value field from the suite file.

        Takes a value from the suite file, clean up quotes, and unpack
        it into list if necesary.

        Args:
            value (str): value to clean up
            remove_quotes ((:obj:`bool`, optional): whether to remove
                single and double quotes.  Defaults to True.
            split_on_comma ((:obj:`bool`, optional): whether to split
                the entry into a list using commas.  Defaults to False
                which implies the output should be a string.

        Returns:
            obj: a cleaned up string if split_on_comma is False or a
                list of strings if True
        """

        value = value.strip()
        if remove_quotes:
            value = _remove_quotes(value)

        if split_on_comma:
            # Remove brackets and split on comma
            value = value.replace("[", "").replace("]", "")
            value = [i.strip() for i in value.split(",")]

        return value

    def parse_rose_suite_run(self):

        """Parse rose-suite-run.conf file.

        Parses the contents of the config file and extracts a number
        of keywords and uses them to set various attributes:

        * site gets set to the local site name
        * groups gets set to a list containing the run_names
        * fcm, cylc, rose get set to their version number (if any)
        * host_xcs gets set to true if it matches a group or variable setting

        Finally, require_comparisons gets set if both COMPARE_OUTPUT
        and COMPARE_WALLCLOCK are set to True in the conf file.
        """

        compare = {}

        with open(self.cylc_version.rose_suite_run_path, encoding="utf-8") as source:
            for line in source:
                if "=" not in line:
                    # Ignore non-keyword lines
                    continue

                key, value = line.strip().split("=", 1)

                if key == "SITE":
                    self.site = self.unpack_suite_value(value)

                elif key == "RUN_NAMES":
                    self.groups = self.unpack_suite_value(value, False, True)

                elif key in ("FCM_VERSION", "CYLC_VERSION", "ROSE_VERSION"):
                    target = key.lower().split("_")[0]
                    value = self.unpack_suite_value(value)
                    if value != "":
                        setattr(self, target, value)

                elif key in ("COMPARE_OUTPUT", "COMPARE_WALLCLOCK"):
                    compare[key] = "true" in value.lower()

                elif ((key == "METO_HPC_GROUP" and "xcs" in value)
                      or "HOST_XC40='xcsr'" in line):
                    self.host_xcs = True

        # This test is a little problematic when running this script
        # on a JULES rose-stem suite as JULES has no 'need' of the two
        # compare variables and to prevent the warning their absence
        # would produce from occuring unnecessarily in JULES they have
        # been added to rose-suite.conf for now
        self.required_comparisons = all(compare.values())

    def initialise_projects(self):

        """Initiase projects from FCM keywords.

        Uses the output from fcm kp to initialise the projects
        attribute with a dictionary containing project names and URLs.
        """

        fcm_exec = FCM[self.site]

        projects = {}
        _, stdout, _ = _run_command([fcm_exec, "kp"])
        find_primary_loc = re.compile(r"location{primary}")
        find_projects = re.compile(r"\[(.*)\]\s*=\s*(.*)")
        find_x_keyword = re.compile(r".x$")
        find_xm_keyword = re.compile(r".xm$")
        find_srs_url = re.compile(r"https://code.metoffice")
        find_mirror_url = re.compile(r"svn:|https://")
        for line in stdout:
            if not find_primary_loc.search(line):
                continue
            result = find_projects.search(line)
            if result:
                project = result.group(1)
                url = result.group(2)
                # Check for keywords conforming to the meto prescribed pattern
                # of ending in '.x' for the external repo and '.xm' for the
                # local mirror.
                if (
                    find_x_keyword.search(project) and find_srs_url.match(url)
                ) or (
                    find_xm_keyword.search(project)
                    and find_mirror_url.match(url)
                ):
                    projects[project] = url
        self.projects = projects

    def export_file(self, repo_url, fname, outname=None):

        """Exports a named file from an FCM project.

        Uses fcm to export a file from a project.  Tries up to five
        times account for any network glitches.

        Args:
            repo_url (str): FCM repository name of the project,
                such as fcm:um.xm_tr for the Unified Model
            fname (str): path to the target file in the repository
            outname ((:obj:`path`, optional): path to the output file
                on the local system.  Defaults to None which implies
                the file should be written to the temporary diretory
                with a unique temporary name.  Using temporary
                locations should be sufficient in most cases.

        Returns:
            Path: the extracted file or None if the export has failed.
        """

        if outname is None:
            # Default to a temporary file in a managed temp directory
            _, outname = mkstemp(prefix="export.", suffix=".txt",
                                 dir=self.tmpdir.name)

        fname = fname.lstrip("/")

        # Try 5 times, if all fail then use working copy version
        for _ in range(5):
            try:
                subproc = f"fcm export -q {repo_url}/{fname} {outname} --force"
                subprocess.check_output(subproc, shell=True)
                return Path(outname)
            except subprocess.CalledProcessError as error:
                print(error)

        return None

    def get_current_code_owners(self, fname):

        """Get the code/config owners file.

        Attempts to get a current owners file from the head of the
        repository or, if this fails, it uses the same file from the
        working copy.

        Args:
            fname (Path): relative to the owners file

        Returns:
            Path: path to a copy of the owners file
        """

        # Export the Owners file from the HOT
        file_path = self.export_file("fcm:um.xm_tr", fname)
        if file_path is None:
            # Couldn't check out file - use working copy Owners file instead
            wc_path = get_working_copy_path(
                self.job_sources["UM"]["tested source"]
            )
            if not wc_path:
                # Empty value means use current working directory
                wc_path = ""
            file_path = Path(wc_path) / fname
            print(f"Using the checked out version of {fname} file")

        return Path(file_path)

    def generate_owner_dictionary(self, mode):

        """Parser an owners file into a diction.

        Args:
            mode (str): the type of the configuration being parsed.
                Valid options are config or code.

        Returns:
            dict: a dictionary where the keys are section names and
                the values are tuples containing a string of owners
                and a string of intersted parties.
        """

        if mode == "config":
            fname = "ConfigOwners.txt"
            sep = "Configuration"
        elif mode == "code":
            fname = "CodeOwners.txt"
            sep = "Owner"
        else:
            return None

        # Get a current version of the owners file
        file_path = self.get_current_code_owners(fname)

        # Read through file and generate dictionary
        try:
            with open(file_path, "r", encoding="utf-8") as inp_file:
                owners_dict = {}
                inside_listing = False
                for line in inp_file:
                    if "{{{" in line:
                        inside_listing = True
                        continue

                    if "}}}" in line:
                        inside_listing = False
                        continue

                    if (inside_listing
                        and line != "\n"
                        and sep not in line):
                        dummy_list = line.split()
                        section = dummy_list[0].strip()
                        owners = dummy_list[1].strip()
                        if "umsysteam" in owners:
                            owners = "!umsysteam@metoffice.gov.uk"
                        try:
                            others = dummy_list[2].replace("\n", "")
                            if others == "--":
                                others = ""
                        except IndexError:
                            others = ""
                        owners_dict.update(
                            {section.lower(): [owners, others]}
                        )
        except IOError:
            print(f"Can't find a valid copy of {fname} file")
            return None

        return owners_dict

    def create_approval_table(self, needed_approvals, mode, output=sys.stdout):

        """Build a code or config owners approval table.

        Builds an approvals table using either code owners or
        configuration owners information and writes it to the output
        stream.

        Args:
            needed_approvals (dict): keys should be owners and values
                should be a list of sections which need approving
            mode (str): the type of the configuration being generated.
                Valid options are config or code.
            output (:obj:`io`, optional): an output stream.  Defaults
                to sys.stdout if not specified.
        """

        if mode == "config":
            columns = ["Owner", "Approval", "Configs"]
        else:
            columns = ["Owner (Deputy)", "Approval", "Code Section"]

        table = self.format_trac_table(
            columns=columns,
            title="Required " + mode.capitalize() + " Owner Approvals",
            output=output)

        # Initialise the table
        table.send(None)

        if needed_approvals is None:
            # No approvals needed
            table.send([None, None, f"No UM {mode.capitalize()} Owner Approvals Required"])

        else:
            for owner in needed_approvals.keys():
                # Add the approvals for each owner, maximum of three per line
                approvals = ""
                for i, what in enumerate(needed_approvals[owner]):
                    if i != 0 and i % 3 == 0:
                        approvals += "[[br]]"
                    approvals += "{{{" + what + "}}} "

                table.send([owner, "Pending", approvals])

        # Always add a trailing newline
        print("", file=output)

    def get_config_owners(self, failed_configs, config_owners):
        """Build a dictionary of required approvers.

        Takes a list of failed configurations and a dictionary of
        configuration owners and uses them to create a dictionary
        holding details of people who need to approve the change.

        Args:
            failed_configs (list): a list of strings holding the names
                of the failed jobs.
            config_owners (dict): the keys are section names and the
                values are tuples containing a string of owners and a
                string of intersted parties.

        Returns:
            dict: a dictionary where the keys are the names of
                configuration owners and the values are a set of
                configuration names.
        """

        # Dictionary to store needed approvals
        needed_approvals = defaultdict(set)

        for job in failed_configs:
            job = job.lower()

            # Split the job name into it's various 'parts'
            parts = job.split("-")

            # Get the Config Name
            # try-except as mule rose-ana names follow different format
            try:
                config = parts[2]
            except IndexError:
                config = "mule" if "mule" in job else ""

            # Get the config owner + others to notify
            try:
                owners = config_owners[config]
            except KeyError:
                owners = ["Unknown", ""]
            owner = owners[0]
            notify = owners[1]

            # If others to notify, append names to config name
            if notify != "":
                config += "(" + notify + ")"

            # Record Owner and Config
            needed_approvals[owner].add(config)

        return needed_approvals

    def required_config_approvals(self, failed_configs, output=sys.stdout):

        """Create a table of required configuration approvals.

        Gets the configuration owners and uses the list of failed
        configurations to write a table of required configuration
        approvals to the output stream.

        Args:
            failed_configs (list): a list of strings holding the names
                of the failed jobs.
            output (:obj:`io`, optional): an output stream.  Defaults
                to sys.stdout if not specified.
        """

        config_owners = self.generate_owner_dictionary("config")
        if config_owners is None:
            return

        config_approvals = self.get_config_owners(failed_configs,
                                                  config_owners)

        if len(config_approvals.keys()) == 0:
            config_approvals = None

        self.create_approval_table(config_approvals, "config", output)

    @staticmethod
    def lookup_ownership_section(fle):

        """Lookup table of some common ownerships.

        Uses a simple table to look up section ownerships based on
        filenames.

        Args:
            fle (str): name of the target file

        Returns:
            str: the owner of the section or an empty string if the
                owner is unknown.
        """

        section = ""

        if fle.startswith("fcm-make"):
            section = "fcm-make_um"

        elif fle.startswith("fab"):
            section = "fab"

        elif fle.startswith("rose-stem"):
            if "umdp3_check" in fle:
                section = "umdp3_checker"
            elif "run_cppcheck" in fle:
                section = "run_cppcheck"
            elif "rose-stem/bin" in fle:
                section = "rose_bin"
            else:
                section = "rose_stem"

        elif fle.startswith("rose-meta"):
            if "versions.py" in fle:
                section = "upgrade_macros"
            elif "rose-meta.conf" in fle:
                section = "rose-meta.conf"
            else:
                section = "stash"

        # Unidentified section
        return section

    def get_file_section_header(self, fpath):

        """Get section ownership from a file header.

        Checks the header information of the target file for an
        embedded section identifier.

        Args:
            fpath (Path): path of the file being queried

        Returns:
            str: the name of the section or an empty string if not
                found.
        """

        # Find area of files in other directories
        file_path = self.export_file("fcm:um.xm_tr", fpath)
        if file_path is None:
            return ""

        section = ""

        try:
            with open(file_path, "r", encoding="utf-8") as inp_file:
                for line in inp_file:
                    if "file belongs in" in line:
                        section = line.strip("\n")
                        break

        except IOError:
            pass

        # Remove C-style comment characters, if any
        section = section.replace("/*", "").replace("*/", "")

        try:
            section = section.split(":")[1].strip().lower()
        except IndexError:
            section = ""

        return section

    def get_code_owners(self, code_owners):

        """Get code owners approvals using fcm_bdiff.

        Uses the output of fcm bdiff to identify the files changed and
        matches this to the code owners for each section.

        Args:
            code_owners (dict): the keys are section names and the
                values are tuples containing a string of owners and a
                string of intersted parties.

        Returns:
            dict: a dictionary where the keys are the names of
                code owners and the values are a set of
                section names.
        """
        # Get list of altered files and exit if no files changed
        # 'UM' used here and just below as this function is
        # currently only valid for the UM. Hopefully lfric_apps
        # will be able to use similar in the future - at this
        # point we can change 'UM' to self.primary_project
        bdiff_files = self.job_sources["UM"]["bdiff_files"]
        if len(bdiff_files) == 0:
            return None

        # Get the mirror repo and remove the @REVISION part
        repo_loc = self.job_sources["UM"]["repo mirror"].split("@")[0]

        # Dictionary to store needed approvals
        needed_approvals = defaultdict(set)

        # Get Owners for each file changed
        for fle in bdiff_files:
            if ".." in fle:
                # This is to fix an invalid path returned by fcm_bdiff in the
                # case a branch has been reversed off trunk (see comments in
                # get_branch_diff_filenames() for detail)
                # The file path (fle) is split by the first example of the
                # branch_name and then the file path as we expect is the last
                # value of that list. We then remove any trailing '/'.
                branch_name = repo_loc.split("/")[-1]
                fle = fle.split(branch_name, 1)[1].strip("/")
            fpath = fle
            fle = fle.lower()

            # Manually sort directories with known sections
            if "configowners.txt" in fle or "codeowners.txt" in fle:
                continue
            if fle.startswith("admin"):
                needed_approvals["!umsysteam@metoffice.gov.uk"].add("admin")
                continue
            if fle.startswith("bin"):
                needed_approvals["!umsysteam@metoffice.gov.uk"].add("bin")
                continue

            section = self.lookup_ownership_section(fle)

            if section == "":
                # Find area of files in other directories
                section = self.get_file_section_header(fpath)

            # Compare area name to code owners list
            try:
                owners = code_owners[section]
                owner, deputy = owners
                if len(deputy) > 0:
                    owner += " (" + deputy + ")"
                needed_approvals[owner].add(section)
            except KeyError:
                needed_approvals[
                    "Unknown - ensure section " "is in CodeOwners.txt"
                ].add(section)

        return needed_approvals

    def required_co_approvals(self, output=sys.stdout):
        """Create a table of code owner approvals.

        Args:
            output (:obj:`io`, optional): an output stream.  Defaults
                to sys.stdout if not specified.
        """

        code_owners = self.generate_owner_dictionary("code")
        if code_owners is None:
            return

        code_approvals = self.get_code_owners(code_owners)
        if code_approvals is False:
            return

        self.create_approval_table(code_approvals, "code", output)

    @staticmethod
    def parse_lfric_extract_list(fpath="~/temp.txt"):
        """
        Read through the lfric_extract list and get a list of files and dirs.
        Return a dictionary with keys 'files' and 'dirs'
        """

        fpath = Path(fpath)

        files = []
        dirs = []
        in_include_section = False

        # Jules also depends on the shared metadata files so add those manually
        dirs.append("rose-meta/jules-shared")

        with open(fpath.expanduser(), encoding="utf-8") as input_file:
            for line in input_file:
                line = line.strip()
                if in_include_section:
                    item = line.rstrip("\\").strip()
                    if "." in item.split("/")[-1]:
                        files.append(item)
                    else:
                        dirs.append(item)
                    if not line.endswith("\\"):
                        in_include_section = False
                if "extract.path-incl" in line:
                    in_include_section = True

        return {"files": files, "dirs": dirs}

    def get_lfric_interactions(self, extract_list):

        """Count interactions with LFRic based on an extract list.

        Matches the set of modified files for each project against the
        list of files extracted by LFRic.  If the number of matches is
        greater than zero, LFRic testing is probably necessary.

        Args:
            extract_list (dict): keys are either files or dirs and
                values are lists of extracted files or directories.

        Returns:
            int: number of projects which interact with LFRic
        """

        num_interactions = 0

        for _, details in self.job_sources:
            for mod_file in details["bdiff_files"]:
                if "trunk" in mod_file:
                    mod_file = mod_file.split("trunk/")[-1]
                # Check modified file isn't in extracted files list
                matching_item = False
                if mod_file in extract_list["files"]:
                    matching_item = True
                # Loop over directories extracted
                # Check that the directory doesn't contain the modified file
                for drc in extract_list["dirs"]:
                    if drc in mod_file:
                        matching_item = True
                        break
                if matching_item:
                    num_interactions += 1
                    break

        return num_interactions

    # FIXME: change to write directly to the output buffer?
    @staticmethod
    def write_lfric_testing_message(num_interactions):
        """Mesage stating LFRic testing requirements.

        Args:
            num_interactions (int): number of projects which interact
                with LFRic

        Returns:
            list: strings containing lines of the output message
        """

        message = []

        if num_interactions > 0:
            if num_interactions > 1:
                message += [f"There were {num_interactions} projects "]
            else:
                message += ["There was 1 project "]
            message += [
                "with LFRic Apps interaction.[[br]]LFRic Apps testing is "
                + "'''required''' before this ticket is submitted for review."
            ]
        else:
            message += [
                "No files shared with LFRic Apps have been "
                + "modified.[[br]]LFRic"
                + " Apps testing is not required for this ticket."
            ]

        message.append("")
        return message

    def check_lfric_extract_list(self, output=sys.stdout):
        """Check whether modified files are extracted by LFRic apps.

        Gets the LFRic apps export list and checks against changes in
        the source branches to get a count of the number of projects
        that have file changes that overlap with LFRic.  Finally, add
        a message to the output stream indicating whether LFRic
        testing is likely to be needed.

        Args:
            output (:obj:`io`, optional): an output stream.  Defaults
                to sys.stdout if not specified.
        """

        print("'''LFRic Testing Requirements'''\n", file=output)

        # Export the extract list from the lfric trunk
        extract_list_path = self.export_file(
            "fcm:lfric_apps.xm_tr",
            "build/extract/extract.cfg",
        )

        if extract_list_path:
            try:
                extract_list_dict = self.parse_lfric_extract_list(
                    extract_list_path
                )
            except (EnvironmentError, TypeError, AttributeError):
                # Potential error here changed type between python2 and 3
                extract_list_path = None

        # If the path returned is None, the extract list failed, most likely as
        # the user doesn't have lfric access. In this case return a warning.
        if extract_list_path is None:
            print("Unable to export the lfric Apps extract_list. "
                  + "LFRic Apps testing may be required.[[br]]\n",
                  file=output)
            return

        # FIXME: extract_list_dict is initialised in a conditional
        num_interactions = self.get_lfric_interactions(extract_list_dict)

        print("\n".join(self.write_lfric_testing_message(num_interactions)),
              file=output)

    def gen_code_and_config_table(self, failed_configs, output=sys.stdout):

        """Generate config/code owners table for the UM.

        Adds the config owners and code owners tables to the output stream
        if the run is a UM suite.

        Args:
            failed_configs (list): a list of strings holding the names
                of the failed jobs.
            output (:obj:`io`, optional): an output stream.  Defaults
                to sys.stdout if not specified.
        """

        self.required_co_approvals(output)
        self.required_config_approvals(failed_configs, output)

    @staticmethod
    def key_by_forced_status(item_tuple):
        """Get item sort keys in forced order.

        If the key is in the DESIRED_ORDER list then return the key's
        index from that list, otherwise return the key.  This forces
        the items in the DESIRED order list to be listed first, in the
        order they appear in the list, followed by all the other keys.

        Caveat: it relies on numbers preceeding alphabetic characters
        and all the status keys starting with a letter.

        Args:
            item_tuple (tuple): key and value pair from a dictionary

        Returns:
            str: the sort order
        """
        key = item_tuple[0]
        if key in DESIRED_ORDER:
            return str(DESIRED_ORDER.index(key))
        return key

    def key_by_name_or_status(self, task_item):
        """Get name or status sort key.

        Given a tuple containing a task name and its status, return
        the tuple in its original form if sort_by_name is True
        otherwise return the tuple in (status, name) form.

        Args:
           task_item (tuple): the name and the status of a task

        Returns:
           tuple: either (name, status) or (status, name) depending
               on instance attributes
        """
        if self.sort_by_name:
            return task_item
        return (task_item[1], task_item[0])

    # pylint: disable=too-many-statements
    # pylint: disable=too-many-branches
    # pylint: disable=too-many-locals

    def generate_task_table(self, data, output=sys.stdout):
        """Create a task table for the suite.

        Convert a task dictionary into a table and add it to an output
        buffer, with output levels controlled by the instance
        verbosity attribute.  As this increases, the amount of output
        decreases as more tasks are hidden.

        The sort_by_name attribute determines whether tasks are sorted
        by name or status.

        Output may include two tables.  One details the number of
        tasks with each status type and the other only appears if it
        is not empty and how many tasks of the relevant types have
        been hidden.

        Args:
           data (dict): task states where they keys are the task names
                and the values are states as strings
           output (:obj:`io`, optional): an IO stream of some sort.
                Defaults to sys.stdout
        """

        hidden_counts = defaultdict(int)

        # Write the task table to a different buffer and add it to the
        # output buffer after the summary table.
        task_table = io.StringIO()
        table = self.format_trac_table(["Task", "State"], output=task_table)
        table.send(None)

        hidden = True

        failed_configs = []
        for task, state in sorted(
            list(data.items()), key=self.key_by_name_or_status
        ):
            # Count the number of times task have any given status.
            self.status_counts[state] += 1
            if (self.verbosity >= 1) and task.startswith("housekeep"):
                hidden_counts["''Housekeeping''"] += 1
                continue
            if (self.verbosity >= 2) and task.startswith("gatekeeper"):
                hidden_counts["''Gatekeeping''"] += 1
                continue
            if task.startswith("monitor"):
                hidden_counts["''Monitoring''"] += 1
                continue
            highlight_start = "'''"
            highlight_end = "'''"
            if "succeeded" in state:
                # Omit printing the task if verbosity level is set to omit all
                # successful tasks, or omit them only when all groups run are
                # "common groups"
                highlight_start = ""
                highlight_end = ""
                if (self.verbosity >= 4) or (self.verbosity >= 3 and self.only_common_groups):
                    hidden_counts["'''Succeeded'''"] += 1
                    continue
            elif "rose_ana" in task and "failed" in state:
                # Check if task requires extra care
                for extra_care_string in HIGHLIGHT_ROSE_ANA_FAILS:
                    if extra_care_string in task:
                        highlight_start = (
                            "'''[[span(style=color: #FF00FF, *****"
                        )
                        highlight_end = "***** )]]'''"
                        self.status_counts[PINK_FAIL_TEXT] += 1
                        self.status_counts[state] -= 1
                        break
                else:
                    # Record this as a failed config
                    failed_configs.append(task)

            table.send([task, f"{highlight_start}{state}{highlight_end}"])
            hidden = False

        if hidden:
            table.send([None,
                        "This table is deliberately empty as all tasks "
                        "are hidden"])

        if self.primary_project.lower() == "um":
            # Add the config owners table for the UM
            self.gen_code_and_config_table(failed_configs, output=output)

        print("'''Suite Output'''", file=output)
        self.gen_resources_table(output)

        print("\n |||| '''All Tasks''' || ", file=output)

        table = self.format_trac_table(["Status", "No. of Tasks"], output=output)
        table.send(None)
        for status, count in sorted(
            self.status_counts.items(), key=self.key_by_forced_status
        ):
            table.send([status, count])
        print("", file=output)

        if len(hidden_counts) > 0:
            print(" |||| '''Hidden Tasks''' || ", file=output)

            table = self.format_trac_table(["Type", "No. of Tasks Hidden"], output=output)
            table.send(None)
            for task_type, count in hidden_counts.items():
                table.send([task_type, count])
            print("", file=output)

        # Finally, append the task table to the output buffer
        print(task_table.getvalue(), file=output)

    # pylint: enable=too-many-statements
    # pylint: enable=too-many-branches
    # pylint: enable=too-many-locals

    def generate_project_table(self, output=sys.stdout):
        """Create a project summary table for the suite.

        Creates a table which contains a line for each project,
        indicating where the project source came from and what
        modifications have been applied to it.

        Args:
           output (:obj:`io`, optional): an IO stream of some sort.
                Defaults to sys.stdout
        """

        table = self.format_trac_table(["Project", "Tested Source Tree",
                                     "Repository Location", "Branch Parent",
                                     "Ticket number", "Uncommitted Changes"],
                                    output=output)
        table.send(None)

        for project, proj_dict in sorted(self.job_sources, key=lambda x: x[0]):
            row = [project,
                   self.format_trac_text([proj_dict["tested source"]], None),
                   self.format_trac_text(
                       [proj_dict["human repo loc"], proj_dict["repo loc"]],
                       proj_dict["repo link"],
                   ),
                   self.format_trac_text(
                       [proj_dict["human parent"], proj_dict["parent loc"]],
                       proj_dict["parent link"],
                   )]

            if proj_dict.get("ticket no"):
                project_ticket_link = [
                    f"{project}:{proj_dict['ticket no']}"
                ]
            else:
                project_ticket_link = [None]
            row.append(self.format_trac_text(project_ticket_link, None))

            wc_link = None
            wc_text = None
            if "working copy changes" in proj_dict:
                if proj_dict["working copy changes"]:
                    wc_text = "YES"
                    # pylint: disable=consider-using-f-string
                    wc_link = r"{0:s}/{1:s}/{2:s}/{3:s}?path=log/{4:s}".format(
                        CYLC_REVIEW_URL[self.site],
                        "view",
                        self.suite_owner,
                        self.suitename,
                        proj_dict["version file"],
                    )
                    # pylint: enable=consider-using-f-string
            row.append(self.format_trac_text([wc_text], wc_link, bold=True))
            table.send(row)

    def gen_resources_table(self, output=sys.stdout):

        """Create a table of resource monitoring jobs.

        Locates the jobs in RESOURCE_MONITORING_JOBS and returns a
        trac-formatted table of resources used by those jobs in this
        suite.

        Args:
            output (:obj:`io`, optional): an IO stream of some sort.
                Defaults to sys.stdout
        """

        print("", file=output)

        table = None
        found_nothing = True
        for job in RESOURCE_MONITORING_JOBS[self.site]:
            filename = self.suite_path / "log" / "job" / "1" / job / "NN" / "job.out"

            if filename.is_file():
                wallclock, memory = self.get_wallclock_and_memory(filename)
                if wallclock and memory:
                    if found_nothing:
                        table = self.format_trac_table(["Task", "Wallclock",
                                                     "Total Memory"],
                                                    preamble=[None, "Resource Monitoring Task"],
                                                    output=output)
                        table.send(None)
                        found_nothing = False
                    table.send([job, wallclock, memory])

        if found_nothing:
            print("  No resource monitoring jobs run", file=output)

        print("", file=output)

    @staticmethod
    def get_wallclock_and_memory(filename):

        """Parse wallclock and memory usage from job output.

        Given an output filename read and parse for the wallclock and
        memory.  The elapsed time is in seconds and the memory figure
        is in kilobytes (decimal magnitudes).

        Note: this assumes that the job output includes information
        generated by the Met Office end-of-job reporting tool.

        Args:
            filename (Path): path to the output of a batch job

        Returns:
            tuple: the elapsed time of the application as an integer
                and the amount of memory used in KB.
        """

        wallclock = "Unavailable"
        memory = "Unavailable"
        find_wallclock = re.compile(
            r"PE\s*0\s*Elapsed Wallclock Time:\s*(\d+(\.\d+|))"
        )
        find_total_mem = re.compile(r"Total Mem\s*(\d+)")
        find_um_atmos_exe = re.compile(r"um-atmos.exe")
        check_for_percentage = re.compile("[0-9]+[%]")
        find_mem_n_units = re.compile(
            r"(?P<num>[0-9]*\.[0-9]*)(?P<unit>[A-Za-z])"
        )

        with open(filename, encoding="utf-8") as source:
            for line in source:
                result = find_wallclock.search(line)
                if result:
                    wallclock = int(round(float(result.group(1))))
                result = find_total_mem.search(line)
                if result:
                    memory = int(result.group(1))
                if find_um_atmos_exe.match(line):
                    split_line = line.split()
                    if check_for_percentage.search(split_line[6]):
                        mem = find_mem_n_units.search(split_line[5])
                        memory = float(mem.group("num"))
                        if mem.group("unit") == "G":
                            memory *= 1000000
                        elif mem.group("unit") == "M":
                            memory *= 1000
                    else:
                        memory = int(line.split()[6])

        return wallclock, memory

    @staticmethod
    def generate_groups(grouplist):
        """Convert the groups run into a Trac formatted string.

        Args:
            grouplist (list): strings containing the names of the
                groups used to run the test suite.

        Returns:
            str: groups separate by Trac line breaks
        """

        output = ""
        for group in grouplist[:-1]:
            output += f"{_remove_quotes(group)} [[br]]"
        output += _remove_quotes(grouplist[-1])
        return output

    def get_project_tickets(self):

        """Get all tickets associated with each project.

        Returns:
            str: space separated details of each ticket
        """

        ticket_nos = ""

        # Check to see if any of the soucres have associated tickets and
        # put links to them in the header if so.
        for project, url_dict in self.job_sources:
            if url_dict.get("ticket no") is not None:
                ticket_nos += f"{project}:{url_dict['ticket no']} "

        return ticket_nos

    def report_uncommited_changes(self, output=sys.stdout):

        """Report on any uncommitted changes.

        Uses the value of the uncommitted_changes attribute to
        determine whether to add a warning about the validity of the
        testing.  Unless all changes are committed, the working copy
        may pass testing but the branch may be broken.

        Args:
            output (:obj:`io`, optional): an IO stream of some sort.
                Defaults to sys.stdout
        """

        print("\n", file=output)
        print("-----", file=output)
        print(" = WARNING !!! = ", file=output)
        if self.uncommitted_changes > 1:
            word = "changes"
        else:
            word = "change"
            print(
                "This rose-stem suite included "
                + f"{self.uncommitted_changes} uncommitted"
                + f" project {word} and is therefore "
                + "'''not valid''' for review",
                file=output)
            print("-----", file=output)
            print("", file=output)

        if (
            not self.required_comparisons
            and "LFRIC_APPS" not in self.job_sources
        ):
            print("", file=output)
            print("-----", file=output)
            print(" = WARNING !!! = ", file=output)
            print(
                "This rose-stem suite did not run the "
                + "required comparisons (COMPARE_OUTPUT "
                + "and/or COMPARE_WALLCLOCK are not true) "
                + "and is therefore '''not valid''' for "
                + "review", file=output
            )
            print("-----", file=output)
            print("", file=output)

    def report_multi_branches(self, output=sys.stdout):

        """Report on the use of multiple branches.

        Args:
            output (:obj:`io`, optional): an IO stream of some sort.
                Defaults to sys.stdout
        """

        print("", file=output)
        print("-----", file=output)
        print(" = WARNING !!! = ", file=output)

        print(
            "This rose-stem suite included multiple "
            + "branches in {len(self.multi_branches)} projects:",
            file=output
        )
        print("", file=output)

        for project, branch_names in self.multi_branches.items():
            print(f"'''{project}'''", file=output)
            for branch_name in "".join(branch_names).split():
                print(f" * {branch_name}", file=output)

        print("", file=output)
        print("-----", file=output)
        print("", file=output)

    def report_header(self, output=sys.stdout):

        """Add a header summary table to the report.

        Creates a table which contains key details about the suite
        including the various options and platform details used for
        testing and outputs it the buffer.  Items are not added to
        table if their value is empty.

        Args:
            output (:obj:`io`, optional): an IO stream of some sort.
                Defaults to sys.stdout
        """

        ticket_nos = self.get_project_tickets()

        title = ""
        if ticket_nos != "":
            title = f"Ticket {ticket_nos} "
        title += "Testing Results - rose-stem output"

        header = self.format_trac_header(title, output)
        header.send(None)

        header.send(["Suite Name", self.suitename])
        header.send(["Suite Owner", self.suite_owner])
        header.send(["Trustzone", self.trustzone])
        header.send(["FCM version", self.fcm])
        header.send(["Rose version", self.rose])
        header.send(["Cylc version", self.cylc])
        header.send(["Report Generated", self.creation_time])

        # pylint: disable=consider-using-f-string
        header.send(["Cylc-Review",
                     "{0:s}/{1:s}/{2:s}/?suite={3:s}"
                     .format(
                         CYLC_REVIEW_URL[self.site],
                         "taskjobs",
                         self.suite_owner,
                         self.suitename)])
        # pylint: enable=consider-using-f-string

        header.send(["Site", self.site])
        header.send(["Groups Run", self.generate_groups(self.groups)])
        header.send(["''ROSE_ORIG_HOST''", self.rose_orig_host])
        header.send(["HOST_XCS", self.host_xcs])

    def print_report(self, output):

        """Print a formatted reort of the suite_report object.

        Generates the complete suite report by calling each of the
        reporting methods in order.  Each of these adds its own
        section to the output buffer to assemble the final report.

        Args:
            output (:obj:`io`, optional): an IO stream of some sort.
                Defaults to sys.stdout
        """

        # pylint: disable=consider-using-f-string
        print("{{{{{{#!div style='background : {0:s}'".format(
            BACKGROUND_COLOURS[self.primary_project.lower()]),
              file=output)
        # pylint: enable=consider-using-f-string

        # Add the summary header
        self.report_header(output)
        print("", file=output)

        if self.uncommitted_changes:
            self.report_uncommited_changes(output)

        if self.multi_branches:
            self.report_multi_branches(output)

        self.generate_project_table(output=output)
        print("", file=output)

        # Check whether lfric shared files have been touched
        # Not needed if lfric the suite source
        if ("LFRIC" not in self.primary_project
            and self.primary_project != "UNKNOWN"):
            self.check_lfric_extract_list(output)

        data = self.cylc_version.task_states()

        self.generate_task_table(data, output=output)
        print("}}}", file=output)

    def write_final_report(self, trac_log):

        """Write the buffered final report.

        Takes the contents of a buffered IO instance and writes it out
        to TRAC_LOG_FILE.  If an IOError occurs while writing to the
        output file, an error message and the contents of the buffer
        are printed to the stdout stream, ensuring the user always
        gets something.

        Args:
            output (io): the IO buffer stream, typically an
                io.StringIO instance used to buffer the
                report ready for printing
        """

        trac_log_path = (self.log_path
                         if self.log_path else
                         self.suite_path) / TRAC_LOG_FILE

        # Attempt to provide user with some output,
        # even in event of serious exceptions
        try:
            with open(trac_log_path, "w", encoding="utf-8") as fd:
                print(trac_log.getvalue(), file=fd)

        except IOError:
            print(
                f"[ERROR] Writing to {TRAC_LOG_FILE} file : {trac_log_path}"
            )
            print(
                f"{TRAC_LOG_FILE} to this point "
                + "would have read as follows :\n"
            )
            print(f"----- Start of {TRAC_LOG_FILE} -----")
            print(trac_log.getvalue())
            print(f"\n----- End of {TRAC_LOG_FILE} -----\n\n")

            raise

# pylint: enable=too-many-instance-attributes
# pylint: enable=too-many-public-methods

# ==============================================================================
#    End of   "class.SuiteReport()"
# ==============================================================================


def get_working_copy_path(path):

    """Get a working copy from a location.

    Args:
        path (str): location of the working copy.  The value provided
            by the script contains the hostname in the format
            <hostname>:<path>.  Python seems unable to parse this
            hence the code below.

    Returns:
        Path: the working copy or None if one could not be found
    """
    working = Path(path)

    if working.exists():
        return working

    # Try to split off a leading hostname portion
    working = Path(path.split(":", 1)[-1])
    if working.exists():
        return working

    return None


def directory_type(opt):

    """Check location exists and is a directory.

    Args:
        opt (str): option value from ArgumentParser

    Returns:
        Path: the absolute location of the diretory with the name
            fully resolved and all symbolic links dereferenced

    Raises:
        ArgumentTypeError: if the value does not refer to an existing
            directory or if it exists but is not a directory.  This
            type of exception is handled by ArgumentParser and generates
            a meaningful error message.
    """

    opt = Path(opt)

    if not opt.exists():
        raise ArgumentTypeError(f"location {str(opt)} does not exist")

    if not opt.is_dir():
        raise ArgumentTypeError(f"location {str(opt)} is not a directory")

    # Return canonical directory with symlinks fully resolved
    return opt.absolute()


def parse_arguments():

    """Process command line arguments.

    Returns:
        Namespace: an instance containing the switches and options
            passed to the script on the command line.
    """

    suite_path = os.environ.get(
        # Cylc7 environment variable
        "CYLC_SUITE_RUN_DIR",
        # Default to Cylc8 environment variable
        os.environ.get("CYLC_WORKFLOW_RUN_DIR", None)
    )

    parser = ArgumentParser(usage="%(prog)s [options] [args]",
                            description=__doc__,
                            formatter_class=RawDescriptionHelpFormatter)

    paths = parser.add_argument_group("location arguments")

    item = paths.add_argument("-S", "--suite-path",
                              type=directory_type,
                              dest="suite_path",
                              metavar="DIR",
                              default=suite_path,
                              help="path to suite")
    if COMPLETION:
        item.completer = argcomplete.DirectoriesCompleter()

    item = paths.add_argument("-L", "--log_path", type=directory_type,
                              dest="log_path",
                              metavar="DIR",
                              help=f"output dir for {TRAC_LOG_FILE}")
    if COMPLETION:
        item.completer = argcomplete.DirectoriesCompleter()

    verbose = parser.add_argument_group("diagnostic arguments")

    verbose.add_argument("-v", "--increase-verbosity",
                         dest="increase_verbosity",
                         action="count",
                         default=0,
                         help="increases Verbosity level. "
                         f"(default: {DEFAULT_VERBOSITY})")

    verbose.add_argument("-q", "--decrease-verbosity",
                         dest="decrease_verbosity",
                         action="count",
                         default=0,
                         help="decreases Verbosity level.")

    misc = parser.add_argument_group("misc arguments")

    misc.add_argument("-N", "--name-sort",
                      dest="sort_by_name",
                      action="store_true",
                      help="sort task table by task names")

    opts, rest = parser.parse_known_args()

    # The calculation below seems counter-intuative. Lower 'verbosity' score
    # actually means a higher verbosity. So SUBTRACT the count for
    # increase_verbosity and ADD the count for decrease_verbosity.
    opts.verbosity = (
        DEFAULT_VERBOSITY - opts.increase_verbosity + opts.decrease_verbosity
    )

    if len(rest) not in (0, 3):
        # If running interactively, there should be zero positional
        # arguments.  If running from a suite's shutdown handler,
        # there should be three arguments.  Anything else should
        # trigger an error.
        parser.error("expected exactly zero or three positional arguments")

    if opts.suite_path is None:
        # Should only happen if environment variables are not set and
        # option has been ommitted
        parser.error("path to suite not provided")

    return opts


def main():
    """Main program.
    Sets up a SuiteReport object and calls it's print_report method."""
    opts = parse_arguments()

    trac_log = io.StringIO()

    try:
        # Handle setup and query errors first because there is nothing
        # useful in the trac.log buffer if a failure occurs at this
        # point
        suite_report = SuiteReport(
            suite_path=opts.suite_path,
            log_path=opts.log_path,
            verbosity=(opts.verbosity),
            sort_by_name=opts.sort_by_name,
        )

    except IOError as err:
        # FIXME: handle IO and environment errors gracefully
        print(f"[ERROR]: {err}")
        raise SystemExit(1) from err

    try:
        # Handle report generation errors differently because there
        # might be something useful in the report buffer
        suite_report.print_report(trac_log)

    except Exception as err:
        # Log the fact that an exception has occurred and that the
        # details are in the cylc scheduler log.  If the exception
        # is caught and reported here, the traceback lacks
        # information about the caller

        print("There has been an exception in SuiteReport. ",
              "See output for more information",
              "rose-stem suite output will be in the files :\n",
              f"{suite_report.cylc_version.suite_scheduler_log_path}\n",
              file=trac_log)

        # Write traceback to the log
        traceback.print_exception(err, file=trac_log)
        raise

    finally:
        # Write the report to an appropriate output file, either
        # the directory specified by the user or the cylc suite
        # directory
        suite_report.write_final_report(trac_log)


if __name__ == "__main__":
    main()
