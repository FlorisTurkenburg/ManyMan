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

import json
import logging

# List of all valid message types
known_msg_types = (
    'client_init',
    'task_start',
    'task_move',
    'task_pause',
    'task_resume',
    'task_stop',
    'task_duplicate',
    'task_output_request',
    # 'core_set_frequency',
    'core_set_voltage'
)

class MessageProcessor:
    """Processor for all messages that arrive in ManyMan's back-end."""

    def __init__(self, server, max_lines):
        self.logger = logging.getLogger('MessageProcessor')
        self.server = server
        self.max_lines = max_lines
        self.logger.debug("Processor inited")

    def process(self, client, msg):
        """Process the given message."""
        try:
            self.logger.debug("MSG: %s" % msg)
            data = json.loads(msg)
            self.logger.debug('JSON: %s' % data)

            if not data['type'] in known_msg_types:
                raise Exception('Invalid message type')
            elif not client.initialized and data['type'] != 'client_init':
                raise Exception('Did not recieve initialization message ' \
                                'first.')
            else:
                getattr(self, "process_" + data['type'])(
                    client,
                    data['content']
                )
        except Exception, e:
            import traceback
            self.logger.warning('Recieved invalid message: %s' % e)
            self.logger.error(traceback.format_exc())
            self.send_invalid(client, e)

    def process_client_init(self, client, msg):
        """Process the client_init message."""
        if client.initialized:
            raise Exception('Already initialized')
        
        client.name = msg['name']
        client.initialized = True
        self.logger.debug('Set client name to %s.' % msg['name'])
        self.send_server_init(client)

    def process_task_start(self, client, msg):
        """Process the task_start message."""
        if 'core' in msg:
            core = int(msg['core'])
        else:
            # Find the best core to start the task on: smart-start
            best = self.server.chip.cores[0]
            for c in self.server.chip.cores[:1]:
                if (c.cpu_usage + c.mem_usage + len(c.tasks)) < \
                    (best.cpu_usage + best.mem_usage + len(best.tasks)):
                    best = c
            core = best.id
        task_id = self.server.chip.add_task(msg['name'], msg['program'], core)
        self.logger.debug('%s started task %s.' % (client.name, task_id))

    def process_task_move(self, client, msg):
        """Process the task_move message."""
        if 'to_core' in msg:
            core = int(msg['to_core'])
        else:
            # Find the best core to move the task to: smart-move
            task = self.server.chip.tasks[msg['id']]
            best = self.server.chip.cores[
                # (task.core + 1) % len(self.server.chip.cores)
                (task.core + 1) % 2
            ]
            for c in self.server.chip.cores[:1]:
                if (c.cpu_usage + c.mem_usage + len(c.tasks)) < \
                    (best.cpu_usage + best.mem_usage + len(best.tasks)) and \
                    c.id != task.core and c.id < 2:
                    best = c
            core = best.id
        self.server.chip.move_task(msg['id'], core)
        self.logger.debug('%s moved task %s.' % (client.name, msg['id']))

    def process_task_pause(self, client, msg):
        """Process the task_pause message."""
        self.server.chip.pause_task(msg['id'])
        self.logger.debug('%s paused task %s.' % (client.name, msg['id']))

    def process_task_resume(self, client, msg):
        """Process the task_resume message."""
        self.server.chip.resume_task(msg['id'])
        self.logger.debug('%s resumed task %s.' % (client.name, msg['id']))

    def process_task_stop(self, client, msg):
        """Process the task_stop message."""
        output = self.server.chip.kill_task(msg['id']) or "No output"
        self.send_task_output(client, msg['id'], output)
        self.logger.debug('%s killed task %s.' % (client.name, msg['id']))

    def process_task_duplicate(self, client, msg):
        """Process the task_duplicate message."""
        self.server.chip.duplicate_task(msg['id'])
        self.logger.debug('%s duplicated task %s.' % (client.name, msg['id']))

    def process_task_output_request(self, client, msg):
        """Process the task_output_request message."""
        output = self.server.chip.get_task_output(msg['id'])
        if 'offset' in msg:
            self.send_task_output(client, msg['id'], output, msg['offset'])
        else:
            self.send_task_output(client, msg['id'], output)
        self.logger.debug(
            '%s requested output of task %s.' % (client.name, msg['id'])
        )

    def process_core_set_frequency(self, client, msg):
        """Process the core_set_frequency message."""
        if 'id' in msg:
            self.server.frequency_scaler.set_core_frequency(
                msg['frequency'],
                msg['id']
            )
        else:
            self.server.frequency_scaler.set_core_frequency(msg['frequency'])

    def process_core_set_voltage(self, client, msg):
        """Process the core_set_voltage message."""
        if 'id' in msg:
            self.server.voltage_handler.set_eCore_voltage(
                msg['voltage'],
                msg['id']
            )
        else:
            self.server.voltage_handler.set_eCore_voltage(msg['voltage'])

    def send_invalid(self, client, error):
        """Send the invalid_message message containing the given error."""
        try:
            msg = {
                'type': 'invalid_message',
                'content': {
                    'message': '%s' % error
                }
            }
            client.request.sendall("%s\n" % json.dumps(msg))
        except Exception, e:
            self.logger.debug('Exception: %s' % e)
        except:
            self.logger.debug('No exception, but still exception...')

    def send_server_init(self, client):
        """Send the server_init message."""
        try:
            msg = {
                'type': 'server_init',
                'content': {
                    'name': self.server.chip.name,
                    'cores': len(self.server.chip.cores),
                    'orientation': self.server.chip.orientation
                }
            }
            if self.server.chip.frequency_tables:
                msg['content']['frequency_tables'] = self.server.chip.frequency_tables
            
            if self.server.voltage_handler:
                msg['content']['voltages'] = self.server.voltage_handler.voltages

            client.request.sendall("%s\n" % json.dumps(msg))
        except Exception, e:
            self.logger.debug('Exception: %s' % e)
        except:
            self.logger.debug('No exception, but still exception...')

    def send_task_output(self, client, task_id, output, offset=0):
        """Send the given task output of the given task with a given offset."""
        try:
            # Maximize the length of the output message
            while(offset < len(output)):
                msg = {
                    'type': 'task_output',
                    'content': {
                        'id': task_id,
                        'output': output[
                            offset:min(len(output), offset + self.max_lines)
                        ]
                    }
                }
                client.request.sendall("%s\n" % json.dumps(msg))
                offset += 100
        except Exception, e:
            self.logger.debug('Exception: %s' % e)
        except:
            self.logger.debug('No exception, but still exception...')
