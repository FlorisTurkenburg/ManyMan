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

from SocketServer import BaseRequestHandler as brh, TCPServer as tcps
from chip import Chip
from messageprocessor import MessageProcessor
from threading import Thread
from time import sleep, time
import SocketServer
import config
import json
import logging
import sys
import subprocess as sp

default_settings = {
    'address': ['', 11111],
    'dummy_mode': False,
    'logging_format': '[%(asctime)s %(levelname)-5s] %(name)s: %(message)s',
    'logging_datefmt': '%B %d, %H:%M:%S',
    'log_filename': 'log',
    'logging_to_console': True,
    'logging_level': 'DEBUG',
    'logging_level_console': 'INFO',
    'max_output_msg_len': 100,
    'status_frequency': 1,
    'frequency_timeout': 3,
    'chip_name': 'ARM big.LITTLE',
    'chip_cores': 8,
    'chip_orientation': [
        [4, 5, 6, 7],
        [0, 1, 2, 3]
    ],
    'frequency_islands': [
        [4, 5, 6, 7],
        [0, 1, 2, 3]
    ],
    'voltage_islands': [
        [4, 5, 6, 7],
        [0, 1, 2, 3]
    ],
    'frequency_table_A7': [
        1400,
        1300,
        1200,
        1100,
        1000,
        900,
        800,
        700,
        600,
        500,
        400,
        300,
        200
    ],
    'frequency_table_A15': [
        2000,
        1900,
        1800,
        1700,
        1600,
        1500,
        1400,
        1300,
        1200,
        1100,
        1000,
        900,
        800,
        700,
        600,
        500,
        400,
        300,
        200
    ]
}


class Client:
    """Client object for storing front-end connections."""

    def __init__(self, request, name):
        self.request = request
        self.name = name
        self.initialized = False


class Server(SocketServer.TCPServer):
    """Server object. Sets up, handles and closes client connections."""

    def __init__(self, address, chip, settings):
        self.logger = logging.getLogger('Server')
        self.chip = chip
        self.settings = settings
        self.connection_count = 0
        self.clients = []
        self.frequency_scaler = None
        self.frequency_thread = None
        self.logger.debug("Initialized on port %d" % address[1])
        tcps.__init__(self, address, MessageHandler)
        self.init_frequency_scaler()
        return

    def init_frequency_scaler(self):
        """Initialize the frequency scaler."""
        self.frequency_scaler = FrequencyScaler(self, self.settings)
        self.frequency_thread = Thread(
            target=self.frequency_scaler.wait_for_assignment
        )
        self.frequency_thread.deamon = True
        self.logger.info("Initialized the FrequencyScaler")

    def serve_forever(self, max_lines):
        """Keep serving client connections."""
        self.frequency_thread.start()
        self.processor = MessageProcessor(self, max_lines)

        self.logger.info("Started")
        try:
            tcps.serve_forever(self)
        finally:
            self.frequency_scaler.running = False
            self.frequency_thread.join()
            self.logger.info('Stopped the FrequencyScaler')
            self.logger.info("Stopped")

    def finish_request(self, request, client_address):
        """A client has successfully connected."""
        self.logger.info("New connection from %s." % client_address[0])
        self.connection_count += 1
        client = Client(request, "Client%d" % self.connection_count)
        self.clients.append(client)
        self.RequestHandlerClass(request, client_address, self, client)

    def close_request(self, request):
        """A client has disconnected."""
        for client in self.clients:
            if client.request == request:
                self.logger.info("Closed connection to %s." % client.name)
                self.clients.remove(client)
                break
        return tcps.close_request(self, request)


class MessageHandler(SocketServer.BaseRequestHandler):
    """Handler for all received messages. Calls the messageprocessor."""

    def __init__(self, request, client_address, server, client):
        self.logger = logging.getLogger('MessageHandler')
        self.client = client
        self.buffer = ""
        brh.__init__(self, request, client_address, server)
        return

    def handle(self):
        """Handle all received messages."""
        while True:
            try:
                data = self.request.recv(1024)
                if not data:
                    break

                if '\n' in data:
                    # A message is not complete until receiving linebreak
                    parts = data.split('\n')
                    self.server.processor.process(
                        self.client,
                        "%s%s" % (self.buffer, parts[0])
                    )

                    # Handle any adjacent fully received messages
                    for part in parts[1:-1]:
                        self.server.processor.process(self.client, part)

                    self.buffer = parts[-1]
                else:
                    self.buffer += data
            except:
                self.logger.error("Exception occurred in MessageHandler")
                break


class StatusSender:
    """Module that sends the chip status at adjustable intervals."""

    def __init__(self, chip, server):
        self.logger = logging.getLogger('StatusSender')
        self.chip = chip
        self.server = server
        self.running = True

    def send_forever(self, interval):
        """Keep sending the status messages on the specified interval."""
        while self.running:
            try:
                sleep(1. / interval)
                msg = {
                    'type': 'status',
                    'content': {
                        'chip': self.chip.as_dict()
                    }
                }
                for client in self.server.clients:
                    client.request.sendall("%s\n" % json.dumps(msg))
            except Exception, e:
                self.logger.warning(
                    'Exception occurred in StatusSender: %s' % e
                )


class FrequencyScaler:

    def __init__(self, server, settings):
        self.server = server
        self.settings = settings
        self.logger = logging.getLogger('FrequencyScaler')
        self.running = True
        self.frequencies = [1400] * 2
        self.last_change = time()
        self.changed = False
        self.changed_island = None

    def wait_for_assignment(self):
        while self.running:
            sleep(1)
            if self.changed:
                self.update_frequencies()

    def update_frequencies(self):
        self.logger.info("Updating frequencies")

        # if self.changed_island != None:
        #     cmd = 'ssh -S ~/.ssh/root@rck00 root@rck00 \'%s -d %s -f %s\'' % (
        #         self.settings['frequency_scale_command'],
        #         self.changed_island,
        #         self.frequencies[self.changed_island]
        #     )
        # else:
        #     cmd = 'ssh -S ~/.ssh/root@rck00 root@rck00 \'%s -c -f %s\'' % (
        #         self.settings['frequency_scale_command'],
        #         self.frequencies[0]
        #     )

        # p = sp.Popen(
        #     cmd,
        #     shell=True,
        #     stdout=sp.PIPE,
        #     stderr=sp.PIPE
        # )

        # # Show output, if any
        # out, err = p.communicate()
        # if len(err) > 0:
        #     self.logger.warning(
        #         "Error when setting frequency: %s" % err
        #     )

        # self.logger.info("out: %s" % out)
        self.changed = False
        self.changed_island = None

    def set_core_frequency(self, f, core=None):
        if time() - self.last_change < self.settings['frequency_timeout']:
            self.logger.warning("Too little time between frequency scalings.")
            return

        # Retrieve available governors
        p = sp.Popen(
                'cpufreq-info -g',
                shell=True,
                stdout=sp.PIPE,
                stderr=sp.PIPE
        )

        # Show output, if any
        out, err = p.communicate()
        if len(err) > 0:
            self.logger.warning(
                "Error when retrieving available governors: %s" % err
            )
            return

        self.logger.debug("cpufreq-info -g : %s" % out)
        if "userspace" not in out.split():
            self.logger.warning(
                "Userspace governor is not available!\n Cannot change "\
                "frequency."
            )
            return


        if core != None:
            for i, island in enumerate(self.settings['frequency_islands']):
                if core in island:
                    break

            if self.frequencies[i] == f:
                return

            self.frequencies[i] = f
            self.changed_island = i
            for c in self.settings['frequency_islands'][i]:
                self.server.chip.cores[c].frequency = f
        else:
            for i in xrange(len(self.settings['frequency_islands'])):
                self.frequencies[i] = f
            for c in self.server.chip.cores:
                c.frequency = f

        self.changed = True
        self.last_change = time()

    def get_core_frequency(self):

        for core in self.server.chip.cores:

            cmd = 'cpufreq-info -c %s -f -m' % core.id

            p = sp.Popen(
                cmd,
                shell=True,
                stdout=sp.PIPE,
                stderr=sp.PIPE
            )

            # Show output, if any
            out, err = p.communicate()
            if len(err) > 0:
                self.logger.warning(
                    "Error when getting frequency: %s" % err
                )
            
            self.logger.info("Core %s: %s" % (core.id, out))


class App:
    """Main application object. Sets up the back-end system."""

    def __init__(self, **kwargs):
        self.settings_file = "settings.cfg"
        if len(sys.argv) > 1 and sys.argv[-1] != '-d':
            self.settings_file = sys.argv[-1]

        self.settings = default_settings.copy()
        self.logger = logging.getLogger('App')
        self.chip = None
        self.server = None
        self.status_sender = None
        self.status_thread = None

        self.load_settings()
        self.settings['dummy_mode'] = kwargs.get('dummy_mode', False)
        self.config_logger()
        self.init_chip()
        self.init_server()
        self.init_status_sender()

        self.serve()

        self.shutdown()

    def load_settings(self):
        """Load settings from settings file."""
        try:
            self.settings.update(config.Config(file(self.settings_file)))
        except Exception, err:
            print 'Settings could not be loaded: %s' % err
            exit(1)

    def config_logger(self):
        """Setup the logger."""
        logging.basicConfig(
            format=self.settings['logging_format'],
            datefmt=self.settings['logging_datefmt'],
            level=getattr(logging, self.settings['logging_level']),
            filename=self.settings['log_filename'],
            filemode='w'
        )

        if self.settings['logging_to_console']:
            # Define console logger
            console = logging.StreamHandler()

            console.setLevel(
                getattr(logging, self.settings['logging_level_console'])
            )
            formatter = logging.Formatter(
                fmt=self.settings['logging_format'],
                datefmt=self.settings['logging_datefmt']
            )
            console.setFormatter(formatter)

            # Add the handler to the root logger
            logging.getLogger('').addHandler(console)

    def init_chip(self):
        """Initialize chip control."""
        self.chip = Chip(
            self.settings['chip_name'],
            self.settings['chip_cores'],
            self.settings['chip_orientation'],
            self.settings['voltage_islands'],
            frequency_tables=[self.settings['frequency_table_A7'],
                self.settings['frequency_table_A15']],
            dummy_mode=self.settings['dummy_mode']
        )
        self.logger.info("Setup chip control")

    def init_server(self):
        """Initialize the server."""
        self.server = Server(
            tuple(self.settings['address']),
            self.chip,
            self.settings
        )
        self.logger.info("Initialized the server")

    def init_status_sender(self):
        """Initialize the status sender."""
        self.status_sender = StatusSender(self.chip, self.server)
        self.status_thread = Thread(
            target=self.status_sender.send_forever,
            args=(self.settings['status_frequency'], )
        )
        self.status_thread.deamon = True
        self.logger.info("Initialized the StatusSender")

    def serve(self):
        """Start the status sender and the server."""
        self.status_thread.start()
        self.logger.info("Started the StatusSender")

        self.logger.info("Starting the server...")
        try:
            self.server.serve_forever(self.settings['max_output_msg_len'])
        except:
            self.logger.warning('Exception in serve')

    def shutdown(self):
        """Shutdown the system."""
        if not self.settings['dummy_mode']:
            self.logger.info("Setting governor back to performance")
            p = sp.Popen('cpufreq-set -c 0-3 -g performance --min %dMHz --max %dMHz '\
                '&& cpufreq-set -c 4-7 -g performance --min %dMHz --max %dMHz ' % (
                    min(self.settings['frequency_table_A7']), 
                    max(self.settings['frequency_table_A7']),
                    min(self.settings['frequency_table_A15']), 
                    max(self.settings['frequency_table_A15'])
                )


                shell=True,
                stdout=sp.PIPE,
                stderr=sp.PIPE
            )

            out, err = p.communicate()
            if len(err) > 0:
                self.logger.warning(
                    "Error setting governor: %s" % err
                )

        self.status_sender.running = False
        self.status_thread.join()
        self.logger.info('Stopped the StatusSender')

        self.chip.stop()
        self.chip.join()
        self.logger.info('Stopped chip control')

if __name__ == "__main__":
    # Run the program. Start in dummy mode with -d arg
    if '-d' in sys.argv:
        App(dummy_mode=True)
    else:
        App()
