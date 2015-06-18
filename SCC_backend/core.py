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

from random import randint
from task import Status as TaskStatus
from threading import Thread
from time import sleep
import logging
import subprocess as sp

class Status:
    """Core statuses."""

    PENDING = 0
    CONNECTING = 1
    RUNNING = 2

    names = ("Pending", "Connecting", "Running")

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return self.names[self.value]


class Core(Thread):
    """Core object, containing all information about a core."""

    def __init__(self, core, **kwargs):
        self.logger = logging.getLogger('Core')

        self.id = core
        self.tasks = dict()
        self.dummy_mode = kwargs.get('dummy_mode', False)

        self.status = Status.PENDING
        self.cpu_usage = 0.0
        self.mem_usage = 0.0
        self._frequency = 533
        self._voltage = 1.

        Thread.__init__(self)

        self.setup()

        self.start()

    def __repr__(self):
        if self.status != Status.RUNNING:
            return "Core %d: connection not yet established." % self.id
        else:
            str_repr = "Core %d: %.1f%% CPU, %.1f%% MEM" % \
                (self.id, self.cpu_usage, self.mem_usage)

            for task in self.tasks.values():
                str_repr += "\n\t%s" % task
            
            return str_repr

    def as_dict(self):
        """Represent the core as a dictionary."""
        if self.status != Status.RUNNING and not self.dummy_mode:
            return {
                "Core": self.id,
                "Status": "%s" % Status(self.status)
            }
        else:
            if self.dummy_mode:
                # Generate pseudo-random resource usage
                self.cpu_usage = min(
                    100,
                    max(0, self.cpu_usage + randint(-10, 10))
                )
                self.mem_usage = min(
                    100,
                    max(0, self.mem_usage + randint(-10, 10))
                )

            dict_repr = {
                "Core": self.id,
                "Status": "%s" % Status(self.status),
                "CPU": self.cpu_usage,
                "MEM": self.mem_usage,
                "Frequency": self.frequency,
                "Voltage": self.voltage
            }
            
            return dict_repr

    def setup(self):
        """Setup a connection to the core and execute top."""
        self.status = Status.CONNECTING

        if not self.dummy_mode:
            self.p = sp.Popen(
                'ssh -M -S ~/.ssh/root@rck%02d root@rck%02d \'top -b -d1\'' % \
                    (self.id, self.id),
                shell=True,
                stdout=sp.PIPE,
                stderr=sp.STDOUT
            )

    def run(self):
        """Continuously read the output of top."""
        if self.dummy_mode:
            self.status = Status.RUNNING
            return

        while 1:
            line = self.p.stdout.readline()

            if not line:
                break

            if self.status == Status.CONNECTING:
                self.status = Status.RUNNING

            self.parse_perf(line)

    def parse_perf(self, line):
        """Retrieve the process data from top's output."""
        if line.lower().find("cpu:") >= 0:
            # Retrieve core ovrall CPU usage
            self.cpu_usage = \
                max(0, 100.0 - float(line.split('%')[3].split()[1]))

        elif line.lower().find("mem:") >= 0:
            # Retrieve core overall memory usage
            self.mem_usage = \
                100 * float(line.split()[1][:-1]) / \
                (float(line.split()[1][:-1]) + float(line.split()[3][:-1]))

        elif len(line.split()) >= 8:
            # Retrieve per task performance data
            parts = line.split()
            pid = parts[0]
            mem = parts[5]
            cpu = parts[6]
            for task in self.tasks.values():
                try:
                    if task.pid == int(pid):
                        task.cpu_usage = float(cpu[:-1])
                        task.mem_usage = float(mem[:-1])
                except:
                    pass

    def add_task(self, t):
        """Add the given task to this core."""
        self.tasks[t.tid] = t
    
    def move_task(self, t, to_core):
        """Move the given task 't' to core 'to_core'.""" 
        if t.status != TaskStatus.CHECKPOINTED:
            if not self.checkpoint_task(t):
                return
        t.restart(to_core.id)
        to_core.add_task(t)

    def checkpoint_task(self, t):
        """Checkpoint the given task."""
        if t.checkpoint():
            while t.status != TaskStatus.CHECKPOINTED:
                # Allow for a context switch
                sleep(.1)
                pass
            self.tasks.pop(t.tid)
            self.logger.debug("Checkpointed!")
            return True
        else:
            return False

    def pause_task(self, t):
        """Pause the given task."""
        if t.stop():
            while t.status != TaskStatus.STOPPED:
                # Allow for a context switch
                sleep(.1)
                pass

    def resume_task(self, t):
        """Resume the given task."""
        if t.cont():
            while t.status != TaskStatus.RUNNING:
                # Allow for a context switch
                sleep(.1)
                pass

    def kill_task(self, t):
        """Kill the given task."""
        if t.kill():
            output = t.output
            t.join()
            return output

    def get_frequency(self):
        """Getter for the core's frequency."""
        return self._frequency

    def set_frequency(self, value):
        """Setter for the core's frequency."""
        if self.frequency == value:
            return

        self._frequency = value

    def get_voltage(self):
        """Getter for the core's voltage."""
        return self._voltage

    def set_voltage(self, value):
        """Setter for the core's voltage."""
        if self.voltage == value:
            return

        self._voltage = value

    # Define getters and setters
    frequency = property(get_frequency, set_frequency)
    voltage = property(get_voltage, set_voltage)
