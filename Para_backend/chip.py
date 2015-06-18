"""
ManyMan - A Many-core Visualization and Management System
Copyright (C) 2015
University of Amsterdam - Computer Systems Architecture
Jimi van der Woning and Roy Bakker
Extended for Parallella by: Floris Turkenburg

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

from core import Core, Status as CoreStatus
from random import randint
from math import floor
import subprocess as sp
from task import Task
from threading import Thread
from time import sleep
import logging
import sys

class Status:
    """Chip statuses."""

    PENDING = 0
    CONNECTING = 1
    RUNNING = 2
    EXITING = 3

    names = ("Pending", "Connecting", "Running", "Exiting")

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return self.names[self.value]


class Chip(Thread):
    """Chip object, containing all information about the entire chip."""

    def __init__(self, name, cores, orientation, voltage_islands, **kwargs):
        self.logger = logging.getLogger('Chip')

        Thread.__init__(self)

        self.name = name
        self.orientation = orientation
        self.voltage_islands = voltage_islands
        self.epiphany_status_dir = kwargs.get('status_dir')
        self.frequency_tables = kwargs.get('frequency_tables', None)
        self.cores = []
        self.tasks = dict()
        self.dummy_mode = kwargs.get('dummy_mode', False)

        self.running = True
        self.status = Status.PENDING
        self.task_count = 0
        self.temp = 0.0

        if self.frequency_tables:
            for i in range(cores):
                self.cores.append(
                    Core(i, frequency_table=self.frequency_tables[int(floor(i/4))], 
                            dummy_mode=self.dummy_mode))

        else:
            for i in range(cores):
                self.cores.append(Core(i, dummy_mode=self.dummy_mode))


        self.status = Status.CONNECTING
        self.start()

    def __repr__(self):
        str_repr =  "%d-core chip:" % len(self.cores)
        
        for core in self.cores:
            str_repr += "\n- %s" % core

        return str_repr

    def as_dict(self):
        """Represent the chip as a dictionary."""
        dict_repr = {
            "Status": "%s" % Status(self.status),
            "Cores": [],
            "Tasks": [],
            "Temperature": self.temp
        }
        
        for core in self.cores:
            dict_repr["Cores"].append(core.as_dict())
        
        for task in self.tasks.values():
            dict_repr["Tasks"].append(task.as_dict())

        self.logger.debug('As dict: %s' % dict_repr)

        return dict_repr

    def run(self):
        """Setup the connection to all cores and retrieve temperature."""


        while self.running:
            sleep(1)

            if not self.running:
                break
            elif self.status == Status.CONNECTING:
                all_active = True
                for core in self.cores:
                    if self.status == Status.EXITING or \
                            core.status != CoreStatus.RUNNING:
                        all_active = False
                        break

                if all_active:
                    self.status = Status.RUNNING
            else:
                # self.get_usage()
                self.get_temp()

    def stop(self):
        """Stop the chip control."""
        self.status = Status.EXITING
        self.running = False

        # Stop all cores and tasks
        for core in self.cores:
            for task in core.tasks.values():
                self.logger.debug("Joining task %s..." % task.name)
                task.kill()
                task.join()
                self.logger.debug("Joined")
            self.logger.debug("Joining core %s..." % core)
            core.join()
            self.logger.debug("Joined")


    def get_usage(self):
        if not self.dummy_mode:
            ecore_status = open('%secore.status' % self.epiphany_status_dir, 'r')
            lines = ecore_status.readlines()
            for core in self.cores[2:]:
                core.cpu_usage = float(lines[core.id-2].split()[2]) / 31.0 * 100.0

            ecore_status.close()

    def get_temp(self):
        """Retrieve the Zynq chip's temperature."""
        if not self.dummy_mode:

            p = sp.Popen('sudo /home/linaro/Documents/ManyMan/Para_backend/gettemp',
                shell=True,
                stdout=sp.PIPE,
                stderr=sp.PIPE
            )
            # Show output, if any
            out, err = p.communicate()
            if len(err) > 0:
                self.logger.warning(
                    "Error when retrieving temperature: %s" % err
                )

            for line in out.split('\n'):
                if "Current Temp" in line:
                    self.temp = float(line.split()[3])
        
        else:
            self.temp = 35.
     

    def add_task(self, name, program, core):
        """Add a task with the given name and program to the given core."""
        self.task_count += 1
        task_id = "T%04d" % self.task_count
        self.logger.debug("Adding task %s" % task_id)
        t = Task(task_id, core, name, program, dummy_mode=self.dummy_mode)
        self.cores[core].add_task(t)
        self.tasks[task_id] = t
        return task_id

    def move_task(self, tid, to_core):
        """Move the task with given Task ID (tid) to core 'to_core'."""
        t = self.tasks[tid]
        if to_core < 0:
            if self.cores[t.core].pause_task(t):
                self.cores[t.core].tasks.pop(t.tid)
                t.core = -1
        elif not self.cores[to_core].eCore:
            self.cores[t.core].move_task(t, self.cores[to_core])

    def pause_task(self, tid):
        """Pause the task with ID 'tid'."""
        t = self.tasks[tid]
        self.cores[t.core].pause_task(t)

    def resume_task(self, tid):
        """Resume the task with ID 'tid'."""
        t = self.tasks[tid]
        self.cores[t.core].resume_task(t)

    def kill_task(self, tid):
        """Kill the task with ID 'tid'."""
        t = self.tasks[tid]
        output = self.cores[t.core].kill_task(t)
        if isinstance(output, list):
            self.tasks.pop(t.tid)
            return output

    def duplicate_task(self, tid):
        """Duplicate the task with ID 'tid'."""
        t = self.tasks[tid]
        if t.core >= 0:
            raise Exception("Running tasks cannot be duplicated.")

        self.task_count += 1
        task_id = "T%04d" % self.task_count
        d = Task(
            task_id,
            -1,
            t.pname,
            t.program,
            dummy_mode=t.dummy_mode,
            status=t._status
        )
        d.output = t.output
        self.tasks[task_id] = d

    def get_task_output(self, tid):
        """Retrieve the output of the task with given tid.""" 
        return self.tasks[tid].output
