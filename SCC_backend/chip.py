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

from core import Core, Status as CoreStatus
from random import randint
import subprocess as sp
from task import Task
from threading import Thread
from time import sleep
import logging

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
        self.cores = []
        self.tasks = dict()
        self.dummy_mode = kwargs.get('dummy_mode', False)

        self.running = True
        self.power_usage = 25
        self.status = Status.PENDING
        self.task_count = 0

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
            "Power": self.power_usage
        }
        
        for core in self.cores:
            dict_repr["Cores"].append(core.as_dict())
        
        for task in self.tasks.values():
            dict_repr["Tasks"].append(task.as_dict())

        self.logger.debug('As dict: %s' % dict_repr)

        return dict_repr

    def run(self):
        """Setup the connection to all cores and retrieve power usage."""
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
                self.get_power()

    def stop(self):
        """Stop the chip control."""
        self.status = Status.EXITING
        self.running = False

        # Stop all cores and tasks
        for core in self.cores:
            for task in core.tasks.values():
                self.logger.debug("Joining task %s..." % task.name)
                task.join()
                self.logger.debug("Joined")
            self.logger.debug("Joining core %s..." % core)
            core.join()
            self.logger.debug("Joined")

    def get_power(self):
        """Retrieve the chip's power consumption."""
        if self.dummy_mode:
            self.power_usage = min(
                125,
                max(25, self.power_usage + randint(-10, 10))
            )
        else:
            p = sp.Popen(
                'sccBmc -c status',
                shell=True,
                stdout=sp.PIPE,
                stderr=sp.PIPE
            )
    
            # Show output, if any
            out, err = p.communicate()
            if len(err) > 0:
                self.logger.warning(
                    "Error when retrieving power: %s" % err
                )

            # Retrieve power usage information
            for line in out.split('\n'):
                if "3V3SCC:" in line:
                    _, u, _, i, _ = line.split()
                    self.power_usage = float(u) * float(i)
                elif "OPVR VCC0:" in line:
                    _, _, i, _ = line.split()
                    for c in self.voltage_islands[3]:
                        self.cores[c].voltage = float(i)
                elif "OPVR VCC1:" in line:
                    _, _, i, _ = line.split()
                    for c in self.voltage_islands[4]:
                        self.cores[c].voltage = float(i)
                elif "OPVR VCC3:" in line:
                    _, _, i, _ = line.split()
                    for c in self.voltage_islands[5]:
                        self.cores[c].voltage = float(i)
                elif "OPVR VCC4:" in line:
                    _, _, i, _ = line.split()
                    for c in self.voltage_islands[0]:
                        self.cores[c].voltage = float(i)
                elif "OPVR VCC5:" in line:
                    _, _, i, _ = line.split()
                    for c in self.voltage_islands[1]:
                        self.cores[c].voltage = float(i)
                elif "OPVR VCC7:" in line:
                    _, _, i, _ = line.split()
                    for c in self.voltage_islands[2]:
                        self.cores[c].voltage = float(i)

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
            if self.cores[t.core].checkpoint_task(t):
                t.core = -1
        else:
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
        d._cfile = t._cfile
        d.output = t.output
        self.tasks[task_id] = d

    def get_task_output(self, tid):
        """Retrieve the output of the task with given tid.""" 
        return self.tasks[tid].output
