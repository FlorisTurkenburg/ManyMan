"""
ManyMan - A Many-core Visualization and Management System
Copyright (C) 2015
University of Amsterdam - Computer Systems Architecture
Jimi van der Woning and Roy Bakker
Extended for big.LITTLE by: Floris Turkenburg

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
        self.frequency_table = kwargs.get('frequency_table', None)
        self.tasks = dict()
        self.dummy_mode = kwargs.get('dummy_mode', False)

        self.status = Status.PENDING
        self.cpu_usage = 0.0
        self.mem_usage = 0.0
        self._frequency = max(self.frequency_table)
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
        """
        Execute mpstat on the given core, and set the cpufreq governor to
        userspace.
        """
        self.status = Status.CONNECTING

        if not self.dummy_mode:
            self.p = sp.Popen(
                'mpstat -P %d 1' % self.id,
                shell=True,
                stdout=sp.PIPE,
                stderr=sp.STDOUT
            )


            sp.Popen(
                'cpufreq-set -c %d -g userspace --min %dMHz --max %dMHz' % (
                    self.id, min(self.frequency_table), max(self.frequency_table)),
                shell=True,
                stdout=sp.PIPE,
                stderr=sp.PIPE
            )


    def run(self):
        """Continuously read the output of mpstat."""
        if self.dummy_mode:
            self.status = Status.RUNNING
            return

        while 1:
            line = self.p.stdout.readline()

            if not line:
                break

            if self.status == Status.CONNECTING:
                self.status = Status.RUNNING

            for task in self.tasks.values():
                try:
                    if task.p_top != None:
                        task_line = task.p_top.stdout.readline()
                        if task_line:
                            task.parse_perf(task_line)                
                except:
                    continue
            
            self.parse_perf(line)
            

    def parse_perf(self, line):
        """Retrieve the process data from mpstat's output."""

        stats = line.split()
        if len(stats) >= 4:
            if stats[1] in ("PM", "AM"):
                if stats[2] == str(self.id):
                    self.cpu_usage = float(stats[3].replace(",", "."))
                    self.logger.debug("Core %d CPU_usage: %f" % (self.id, self.cpu_usage))
            elif stats[1] == str(self.id):
                self.cpu_usage = float(stats[2].replace(",", "."))
                self.logger.debug("Core %d CPU_usage: %f" % (self.id, self.cpu_usage))

        total_mem = 0
        for task in self.tasks.values():
            total_mem += task.mem_usage

        self.mem_usage = total_mem



    def add_task(self, t):
        """Add the given task to this core."""
        self.tasks[t.tid] = t
    
    def move_task(self, t, to_core):
        """Move the given task 't' to core 'to_core'.""" 

        t.move(to_core.id)
        to_core.add_task(t)


    def pause_task(self, t):
        """Pause the given task."""
        if t.stop():
            while t.status != TaskStatus.STOPPED:
                # Allow for a context switch
                sleep(.1)
                pass
            return True
        else:
            return False

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

        p = sp.Popen("cpufreq-set -c %d -f %dMHz" % (self.id, value),
            shell=True,
            stdout=sp.PIPE,
            stderr=sp.PIPE
        )

        out, err = p.communicate()
        self.logger.debug("cpufreq-set response: %s \n%s" % (out, err))
        if len(out) > 0:
            self.logger.warning("Set freq: %s" % out)
            raise Exception("Frequency set failed with errors: %s" % out)
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
