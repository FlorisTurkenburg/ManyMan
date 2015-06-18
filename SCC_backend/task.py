"""
ManyMan - A Many-core Visualization and Management System
Copyright (C) 2012
University of Amsterdam - Computer Systems Architecture
Jimi van der Woning and Roy Bakker

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

from random import random, randint
from string import split
from threading import Thread
from time import sleep
import logging
import subprocess as sp

# Filesystem paths
BLCR_SETUP_PATH = "/shared/jimivdw/blcr_setup"
CONTEXTS_DIR = "/shared/jimivdw/contexts"

class Status:
    """Task statuses."""
    NEW = 0
    CREATING = 1
    RUNNING = 2
    CHECKPOINTING = 3
    CHECKPOINTED = 4
    RESTARTING = 5
    STOPPING = 6
    STOPPED = 7
    CONTINUING = 8
    KILLING = 9
    KILLED = 10
    FINISHED = 11
    FAILED = 12
    
    names = (
        "New",
        "Creating",
        "Running",
        "Checkpointing",
        "Checkpointed",
        "Restarting",
        "Stopping",
        "Stopped",
        "Continuing",
        "Killing",
        "Killed",
        "Finished",
        "Failed"
    )
    
    def __init__(self, value):
        self.value = value
    
    def __repr__(self):
        return self.names[self.value]


class Task(Thread):
    """Task object, contains all information about a task."""

    def __init__(self, tid, core, name, program, **kwargs):
        self.logger = logging.getLogger('Task')

        self.tid = tid
        self.core = core
        self.pname = name
        self.program = program
        self.dummy_mode = kwargs.get('dummy_mode', False)

        self._pid = -1
        self.ppid = -1
        self._cfile = ""
        self._error_count = 0

        self._status = kwargs.get('status', Status.NEW)
        self.cpu_usage = 0.0
        self.mem_usage = 0.0
        self.output = []

        Thread.__init__(self)

        if self._status == Status.NEW:
            self.create()

        self.start()

    def __repr__(self):
        return "Task %s: %.1f%% CPU, %.1f%% MEM" % (
            self.tid,
            self.cpu_usage,
            self.mem_usage
        )

    def as_dict(self):
        """Represent the task as a dictionary."""
        if self.dummy_mode:
            # Generate pseudo-random performance data
            if self._status == Status.RUNNING:
                self.cpu_usage = min(
                    100,
                    max(0, self.cpu_usage + randint(-10, 10))
                )
                self.mem_usage = min(
                    100,
                    max(0, self.mem_usage + randint(-10, 10))
                )

                if randint(0, 9) == 0:
                    self.output += ["Some random number: %.4f\n" % random()]

                # Finish task with a chance of 1/100
                if randint(0, 99) == 0:
                    self.status = Status.FINISHED

                # Fail task with a chance of 1/1000
                if randint(0, 999) == 0:
                    self.status = Status.FAILED
            else:
                self.cpu_usage = max(0, self.cpu_usage - randint(10, 50))
                self.mem_usage = max(0, self.mem_usage - randint(10, 50))

        return {
            "ID": self.tid,
            "Core": self.core,
            "Name": self.pname,
            "Status": "%s" % Status(self.status),
            "CPU": self.cpu_usage,
            "MEM": self.mem_usage
        }

    def create(self):
        """Start this task on its previously specified core."""
        self.logger.debug("Creating a task on core %02d" % self.core)
        self.status = Status.CREATING

        if self.dummy_mode:
            return

        pcall = self.program.split(' ')[0]

        self.p = sp.Popen(
            'if [ -x %s ]; then ssh -S ~/.ssh/root@rck%02d root@rck%02d ' \
                '\'echo "PPID: $$"; source %s; cr_run %s\'; else ' \
                'echo \'ERROR: Program not found\' 1>&2; fi' \
                % (pcall, self.core, self.core, BLCR_SETUP_PATH, \
                self.program),
            shell=True,
            stdout=sp.PIPE,
            stderr=sp.STDOUT
        )

    def run(self):
        """Read and process the output of the task."""
        if self.dummy_mode:
            self.status = Status.RUNNING
            return

        while 1:
            if self.status in (Status.CHECKPOINTED, Status.STOPPED):
                # Skip when checkpointed or stopped
                sleep(.1)
                continue

            line = self.p.stdout.readline()

            if not line:
                if self.status in (Status.CHECKPOINTED, Status.STOPPED):
                    # Skip when checkpointed or stopped
                    continue
                else:
                    # The task will have ended
                    self.logger.debug("Breaking")
                    break

            if self.status in (Status.CREATING, Status.RESTARTING):
                # Setup the task, i.e. retrieve its parent's process id (ppid)
                try:
                    parts = line.split()
                    if parts[0] == "ERROR:":
                        raise ValueError(" ".join(parts[1:]))
                    if parts[0] != "PPID:":
                        if self._error_count > 100:
                            raise ValueError("Could not determine ppid.")
                        self._error_count += 1
                        continue

                    self.ppid = int(parts[1])
                    self.logger.debug("Set ppid to %d" % self.ppid)
                    self.status = Status.RUNNING
                except ValueError as e:
                    self.logger.critical("Could not start the task:\n %s" % e)
                    self.status = Status.FAILED
                    break
            else:
                self.logger.debug("%s: %s" % (self.name, line[:-1]))
                self.output += [line]

        if self.status not in (Status.KILLED, Status.FAILED):
            # The task has successfully ended
            self.status = Status.FINISHED
            self.cpu_usage = 0.0
            self.mem_usage = 0.0

    def checkpoint(self):
        """Checkpoint the task."""
        if self.dummy_mode:
            self.status = Status.CHECKPOINTED
            return True

        if self.status != Status.RUNNING:
            self.logger.warning("Attempting to checkpoint a non-running task.")
            return False

        self.logger.debug("Checkpointing the program...")
        self.status = Status.CHECKPOINTING

        self._cfile = "%s/context.%d.%d.%s.cxt" % \
            (CONTEXTS_DIR, self.core, self.ppid, self.tid)

        # The -d option is yet to be implemented in cr_checkpoint(!)
        p = sp.Popen(
            'ssh -S ~/.ssh/root@rck%02d root@rck%02d \'source "%s"; ' \
            'cr_checkpoint --kill -f %s %d\'' % \
            (self.core, self.core, BLCR_SETUP_PATH, self._cfile, self.pid),
            shell=True,
            stdout=sp.PIPE,
            stderr=sp.PIPE
        )

        # Show output, if any
        out, err = p.communicate()
        if len(err) > 0:
            self.logger.warning(
                "Checkpointing exited with some errors: %s" % err
            )
        if len(out) > 0:
            self.output += [out]

        # Reset all data
        self.ppid = -1
        self._pid = -1
        self.status = Status.CHECKPOINTED
        self.cpu_usage = 0.0
        self.mem_usage = 0.0

        self.logger.debug("Checkpointed the program")

        return True

    def restart(self, core):
        """Restart the task on the specified core."""
        if self.dummy_mode:
            self.core = core
            self.status = Status.RUNNING
            return True

        if self.status != Status.CHECKPOINTED:
            self.logger.warning(
                "Attempting to restart a non-checkpointed task."
            )
            return False

        self.logger.debug("Attempting to restart on core %02d" % core)

        self.p = sp.Popen(
            'ssh -S ~/.ssh/root@rck%02d root@rck%02d \'echo "PPID: $$"; ' \
                'source "%s"; cr_restart --no-restore-pid %s\'' \
                % (core, core, BLCR_SETUP_PATH, self._cfile),
            shell=True,
            stdout=sp.PIPE,
            stderr=sp.STDOUT
        )

        self.core = core
        self.status = Status.RESTARTING
        return True

    def stop(self):
        """Stop the task."""
        if self.dummy_mode:
            self.status = Status.STOPPED
            return True

        if self.status != Status.RUNNING:
            self.logger.warning("Attempting to stop a non-running task.")
            return False

        self.logger.debug("Stopping the program...")
        self.status = Status.STOPPING

        p = sp.Popen(
            'ssh -S ~/.ssh/root@rck%02d root@rck%02d \'kill -SIGSTOP %d\'' % \
            (self.core, self.core, self.pid),
            shell=True,
            stdout=sp.PIPE,
            stderr=sp.PIPE
        )

        # Show output, if any
        out, err = p.communicate()
        if len(err) > 0:
            self.logger.warning("Stopping exited with some errors: %s" % err)
        if len(out) > 0:
            self.output += [out]

        self.status = Status.STOPPED
        self.logger.debug("Stopped the program")
        return True

    def cont(self):
        """Continue the stopped task."""
        if self.dummy_mode:
            self.status = Status.RUNNING
            return True

        if self.status != Status.STOPPED:
            self.logger.warning("Attempting to continue a non-stopped task.")
            return False

        self.logger.debug("Continuing the program...")
        self.status = Status.CONTINUING

        p = sp.Popen(
            'ssh -S ~/.ssh/root@rck%02d root@rck%02d \'kill -SIGCONT %d\'' % \
            (self.core, self.core, self.pid),
            shell=True,
            stdout=sp.PIPE,
            stderr=sp.PIPE
        )

        # Show output, if any
        out, err = p.communicate()
        if len(err) > 0:
            self.logger.warning("Continuing exited with some errors: %s" % err)
        if len(out) > 0:
            self.output += [out]

        self.status = Status.RUNNING
        self.logger.debug("Continued the program")
        return True

    def kill(self):
        """Kill the task."""
        if self.dummy_mode:
            self.status = Status.KILLED
            return True
        
        if self.status not in (Status.RUNNING, Status.STOPPED):
            self.logger.warning("Attempting to kill a non-running task.")
            return False

        self.logger.debug("Killing the program...")
        self.status = Status.KILLING

        p = sp.Popen(
            'ssh -S ~/.ssh/root@rck%02d root@rck%02d \'kill %d\'' \
                % (self.core, self.core, self.pid),
            shell=True,
            stdout=sp.PIPE,
            stderr=sp.PIPE
        )

        # Show output, if any
        out, err = p.communicate()
        if len(err) > 0:
            self.logger.warning("Killing exited with some errors: %s" % err)
        if len(out) > 0:
            self.output += [out]

        self.status = Status.KILLED
        self.logger.debug("Killed the program")
        return True

    def get_child_pid(self, ppid):
        """Retrieve the child PID from its PPID."""
        # Obtain all processes with the given ppid
        p2 = sp.Popen(
            'ssh -S ~/.ssh/root@rck%02d root@rck%02d \'ps -a -o ' \
            'pid,ppid,comm | grep %d\'' %
            (self.core, self.core, ppid),
            shell=True,
            stdout=sp.PIPE,
            stderr=sp.PIPE
        )
        out, err = p2.communicate()
        if len(err) > 0:
            raise Exception("Get_pid exited with errors: %s" % err)

        pid = -1

        for i, line in enumerate(split(out, '\n')[:-1]):
            self.logger.debug("Line %d: %s" % (i, line))

            # Retrieve the pid, ppid and command
            try:
                npid, nppid = split(line)[:2]
                npid = int(npid)
                nppid = int(nppid)
            except:
                self.logger.warning(
                    "Encountered an invalid line of process information..."
                )
                continue

            if npid != self.ppid:
                if pid > 0:
                    self.logger.debug(
                        "Found pid %d after %d was found earlier.." % \
                        (npid, pid)
                    )
                pid = npid

        return pid

    def get_pid(self):
        """Get the current process id of the task."""
        if self.dummy_mode:
            return 9999

        if self.ppid < 0:
            raise Exception(
                "Could not determine the process id, since the parent's " \
                "process id could not be found."
            )

        # Try to get the pid from memory
        if self._pid < 0:
            pid = self.get_child_pid(self.ppid)

            self.logger.debug("Set pid to %d" % pid)
            self._pid = pid

        if self._pid < 0:
            raise Exception("Could not determine the process id.")

        return self._pid

    def get_status(self):
        """Getter for the task status."""
        return self._status

    def set_status(self, value):
        """Setter for the task status. Logs it."""
        self._status = value
        if value in (Status.CREATING, Status.RESTARTING):
            self._error_count = 0
        self.logger.debug("Status changed to %s" % Status(self.status))

    # Define getters and setters
    pid = property(get_pid)
    status = property(get_status, set_status)
